//! SSTable reader implementation: efficient reading of SSTable files in Cassandra 5+
//! format — block-based reading with compression (LZ4/Snappy/Deflate/Zstd), index-based
//! lookups, memory-efficient streaming, and bloom-filter integration.

// Submodules
/// Disk-access backend + madvise resolution (config + `CQLITE_*` env + file
/// size), split out under the campsite rule (epic #1116).
mod backend_resolve;
mod block_io;
mod bti_lookup_memo;
/// Single chunk decode plane (issue #1598, Epic G / G2).
mod chunk_source;
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
pub mod presence_verification; // #2163 opt-in false-negative verification switch
                               // One format-tagged partition-location façade (issue #1599 / G3): `locate`
                               // composes the C5 range short-circuit + per-format offset resolve.
mod partition_locator;
// Next-partition (successor) seek-window offset resolution (issue #953 / #951),
// split out of `partition_lookup` for the campsite source-size rule (#1116).
pub(crate) mod partition_successor;
// Positional (`pread`-style) point-read backends (issue #1573, Epic C / C2).
// Reader-reported open-descriptor accounting for `cqlite.reader.fds.open`
// (issue #1707): an RAII guard stored beside each handle CQLite really opens.
mod fd_gauge;
mod read_at;
// Concurrency scenarios for the ReadAt point-read migration (issue #1573).
#[cfg(all(test, unix))] // mmap advice: #2824 WILLNEED + the #1143 Sequential ban
mod prefetch_advice_tests;
#[cfg(test)]
mod read_at_point_tests;
// Direct-I/O read-ahead window sizing (issue #1596, Epic F / F6): clamps a
// sub-chunk `direct_io_prefetch_bytes` so one compression chunk never straddles
// more than two aligned refills.
mod prefetch_window;
// sync-fallback registry-schema pre-resolution (issue #1692)
#[cfg(feature = "state_machine")]
mod registry_schema;
// Windowed streaming-scan driver (issue #1143); `pub` ONLY under non-default
// `scan-offload-probe` so the #1143 guard reaches its probe, else private.
#[cfg(not(feature = "scan-offload-probe"))]
pub(crate) mod scan_stream_windowed;
#[cfg(feature = "scan-offload-probe")]
pub mod scan_stream_windowed;
mod source;
mod summary_point; // #2412 §B: Summary-guided bounded-interval BIG point lookup
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
// Active-window decode borrow source (issue #1644, K5 stage 2) — localizes the
// borrow-vs-copy decision to one place without threading a window handle
// through the whole decode call graph. See module docs.
pub(crate) mod value_borrow;

// Re-export public types
pub use types::{
    BlockMeta, IntegrityCheckResult, IntegrityStatus, SSTableReader, SSTableReaderConfig,
    SSTableReaderHealthMetrics, SSTableReaderStats,
};
// Re-export the within-partition clustering-slice push-down spec (Issue #954).
pub use data_access::ClusteringSlice;
// Streaming-scan consumer handles (#3106/#3124). The aliases name a type whose defining
// module is NOT publicly nameable, so `JoinedStream`/`ScanStreamItem` ship with them —
// else an external holder of a `RowScanStream` cannot name its type or see `recv()`.
pub use data_access::joined_scan_stream::{
    BatchedScanStream, JoinedStream, RowScanStream, ScanStreamItem,
};
// Token-range bound pushed into the Summary-guided streaming walk (issue #2413
// Option A) — used by the flight warm merge to scope a split's scan.
pub use data_access::{QueryRowBatch, QueryRowStream, ScanTokenBound, QUERY_ROWS_MAX_READ_AHEAD};
// Single-partition compaction seek outcome (issue #2207). `not(tombstones)` like
// the seek path it wraps.
#[cfg(not(feature = "tombstones"))]
pub use data_access::SinglePartitionCompaction;
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
use header::{
    calculate_actual_header_size, extract_generation_from_path, parse_header_with_version_detection,
};

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Mutex;

use source::{BlockSource, ScanSource};

// Disk-access backend resolution (`backend_resolve`). The parse helpers stay
// private to that module and are exercised through it by `reader::tests`.
#[cfg(unix)]
use backend_resolve::mmap_advice_for;
use backend_resolve::{
    direct_io_available, disk_access_mode_via_env, mmap_enabled_via_env, prefetch_mode_via_env,
    resolve_disk_access_mode, system_memory_bytes,
};

use crate::{
    config::{DiskAccessMode, PrefetchMode},
    parser::{header::CassandraVersion, SSTableHeader, SSTableParser},
    platform::Platform,
    schema::TableSchema,
    storage::sstable::{
        compression::CompressionReader,
        compression_info::CompressionInfo,
        version_gate::{BigVersionGates, VersionGates},
    },
    Config, Error, Result, RowKey, ScanRow, Value,
};

// Structured logging
use tracing::debug;

#[cfg(feature = "tombstones")]
use super::tombstone_merger::TombstoneMerger;

/// The `Index.db` hardlink SIBLING of a `Data.db` path (issue #2356 roborev),
/// i.e. the `*-Data.db` name-suffix swapped for `*-Index.db` in the same
/// directory. `None` when the name is not the expected `*-Data.db` shape. Used
/// by [`SSTableReader::rebind_path`] to follow a #2383 inode-rebind for the lazy
/// `Index.db` path, and by the streaming-walk `current_index_db_path` derivation
/// (same rule) so every `Index.db` consumer stays on the rebound generation.
pub(crate) fn index_db_sibling(data_path: &Path) -> Option<PathBuf> {
    let parent = data_path.parent()?;
    let name = data_path.file_name().and_then(|n| n.to_str())?;
    let base = name.strip_suffix("-Data.db")?;
    Some(parent.join(format!("{base}-Index.db")))
}

/// Minimum SSTable size for the point-read path to get its OWN mmap advised `MADV_RANDOM` (issue
/// #2210). Below this the point source shares the scan's `Arc<Mmap>` unchanged (no 2nd mapping; it
/// carries the scan plane's advice): the whole file is small enough that read-ahead cheaply makes
/// it resident and `MADV_RANDOM` would only add per-page faults. Above it, scattered point-lookup
/// faults otherwise waste the ~128 KiB read-ahead window per read, so a dedicated `MADV_RANDOM`
/// mapping collapses both the block-I/O amplification (~30x) and the cold-cache per-read latency
/// (~35-43%). Threshold is measurement-derived on Linux/EBS: the win is unambiguous by 4 MiB; 8
/// MiB leaves 2x margin above the sub-MB "wash" zone. See
/// docs/reports/issue-2210-madv-random-point-mmap-ab.md. The SCAN mapping never gets
/// `MADV_RANDOM`; the default `PrefetchMode::Auto` advises it `MADV_WILLNEED` (async read-ahead,
/// no drop-behind — #2824), `Off` leaves it unadvised, and `Sequential`/`WillNeed` are honoured as
/// named. It backs BOTH `BlockSource::Mapped` and `scan_positional_source`, and the windowed /
/// Summary-guided scan feed reads through the latter (`ReadAt`), NOT through `BlockSource` (#2876)
/// — so a claim scoped to `BlockSource` describes nothing that a real scan does.
#[cfg(unix)]
const POINT_MMAP_MADV_RANDOM_MIN_BYTES: u64 = 8 * 1024 * 1024;

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

// The open constructors + the single failed-open recording boundary (issues #1037,
// #1704). Their own file per the campsite rule (#1116): this one is more than twice
// the ~800-line target.
mod open;
pub(crate) use data_access::joined_scan_stream::ScanErrorReporting;
// Only the `delta-scan` and `write-support` consumers state a reporting mode; with
// both features off nothing can call it, so the re-export carries their cfg rather
// than an `allow(unused_imports)`. A bare allow would convert "no consumer compiled"
// — the signal that the wiring is gone — into silence, on an issue whose whole
// subject is an error path nobody can observe (#1704).
#[cfg(any(feature = "delta-scan", feature = "write-support"))]
pub(crate) use open::OpenErrorReporting;
impl SSTableReader {
    /// Open implementation; see [`open`](Self::open) for the instrumented wrapper.
    async fn open_inner(
        path: &Path,
        config: &Config,
        platform: Arc<Platform>,
        chunk_cache: Arc<crate::storage::cache::DecompressedChunkCache>,
        cancel: crate::storage::scan_cancel::ScanCancel,
    ) -> Result<Self> {
        // #1696 (roborev r2 F4): validate the direct-I/O fraction FIRST, before
        // any `tokio::fs` call. It was validated at its point of use — after
        // `tokio::fs::metadata` had already run — so a missing or unreadable file
        // masked an invalid config with an I/O error, and the caller was told
        // about the wrong problem. A config error needs no bytes to diagnose, so
        // it is diagnosed before the first byte is touched. The value is carried
        // to the resolver below rather than re-derived, keeping
        // `StorageConfig::validated_direct_io_memory_fraction` the single
        // definition of the rule.
        let direct_io_memory_fraction = config.storage.validated_direct_io_memory_fraction()?;
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
                tracing::debug!(
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
            direct_io_memory_fraction, // #1696 F2: rejected above, never clamped
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

        // Build the POINT-READ positional source ONCE at open (issue #1573, C2).
        // It shares the reader's `Arc<Mmap>` when the backend is mmap (no extra
        // mapping / fd); for buffered/direct it holds one dedicated read-only fd,
        // which is exactly the "open the fd once, positioned-read thereafter"
        // contract that removes the BTI per-lookup `open(2)`. Every non-mmap
        // backend degrades gracefully to a plain positioned fd if the faster
        // backend is refused, mirroring `build_block_sources`.
        let point_source: Arc<dyn read_at::ReadAt> = match &scan_source {
            ScanSource::Mapped(mmap) => {
                #[cfg(unix)]
                let point_mmap =
                    Self::point_read_mmap(path, file_size, mmap, POINT_MMAP_MADV_RANDOM_MIN_BYTES);
                #[cfg(not(unix))]
                let point_mmap = mmap.clone();
                Arc::new(read_at::MmapReadAt::new(point_mmap))
            }
            #[cfg(unix)]
            ScanSource::Direct { .. } => match read_at::DirectReadAt::open(path, file_size) {
                Ok(d) => Arc::new(d) as Arc<dyn read_at::ReadAt>,
                Err(e) => {
                    tracing::warn!(
                        "Direct-I/O point source for {} failed ({}); using buffered pread",
                        path.display(),
                        e
                    );
                    Arc::new(read_at::PlainFileReadAt::open(path, file_size)?)
                }
            },
            ScanSource::Buffered { .. } => {
                Arc::new(read_at::PlainFileReadAt::open(path, file_size)?)
            }
        };

        // Build the SCAN-side positional source (issue #2876): the Summary-guided
        // walk and the windowed scan feed must NOT share the deliberately
        // `MADV_RANDOM` `point_source` (#2210) — backwards for a mostly-sequential
        // scan (a 4 KiB page fault per read, not the ~128 KiB read-ahead window).
        // Reuse the SAME never-`MADV_RANDOM` mapping `ScanSource::Mapped` holds (an
        // `Arc` clone, no new mapping / fd); Direct/Buffered share `point_source`.
        let scan_positional_source: Arc<dyn read_at::ReadAt> = match &scan_source {
            ScanSource::Mapped(mmap) => Arc::new(read_at::MmapReadAt::new(mmap.clone())),
            #[cfg(unix)]
            ScanSource::Direct { .. } => point_source.clone(),
            ScanSource::Buffered { .. } => point_source.clone(),
        };

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

        // Load CompressionInfo.db ONCE for chunked decompression (if it exists).
        // This is the single authoritative parse per open (issue #1597 / G1); the
        // component name is derived deterministically from `SsTableDescriptor`
        // (one `exists()` probe, not the legacy ~25-generation scan).
        let compression_info = Self::load_compression_info_metadata(path, &platform).await?;

        // Derive the compression reader (algorithm only) from that single parsed
        // result — no second parse. `algorithm_enum()` is fallible only for a name
        // `parse()` would already have rejected, so no `unwrap`/`expect` is needed.
        let compression_reader = match &compression_info {
            Some(info) => Some(CompressionReader::new(info.algorithm_enum()?)),
            None => None,
        };

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
                tracing::warn!(
                    "Component integrity issues detected but proceeding with loading: {:?}",
                    integrity_issues
                );
            }
        }

        // Integrated in-Data.db index only; separate Index.db is parsed once by load_index_reader (#2385/#2395).
        let index = Self::load_index(&file, &header)
            .instrument(tracing::debug_span!("sstable.reader.open.load_index"))
            .await?;

        // Load bloom filter if available (supports both integrated and component-based)
        let bloom_filter = Self::load_bloom_filter(&file, &header, &platform, path)
            .instrument(tracing::debug_span!("sstable.reader.open.load_bloom"))
            .await?;

        // Issue #2412: load Summary.db FIRST so its usability (present, parsed, at
        // least one sample entry — the authority a bounded lazy walk needs) can
        // gate whether `load_index_reader` defers the Index.db parse (lazy, design
        // §A) or falls back to today's eager parse (§A1's counted FellBack).
        let summary_reader = Self::load_summary_reader(path, &platform)
            .instrument(tracing::debug_span!("sstable.reader.open.load_summary"))
            .await;
        let summary_usable = summary_reader
            .as_ref()
            .map(|s| !s.get_entries().is_empty())
            .unwrap_or(false);

        // Load spec readers for enhanced metadata and lookups. Distinguish an absent
        // Index.db from a present-but-unloadable one (issue #2302) so the full
        // enumeration can WARN loud on the latter instead of silently full-scanning.
        let (index_reader, index_present_but_unloadable) =
            match Self::load_index_reader(path, &platform, &cancel, summary_usable)
                .instrument(tracing::debug_span!(
                    "sstable.reader.open.load_index_reader"
                ))
                .await?
            {
                component_loading::IndexLoadOutcome::Loaded(reader) => (Some(*reader), false),
                component_loading::IndexLoadOutcome::Absent => (None, false),
                component_loading::IndexLoadOutcome::PresentButUnloadable => (None, true),
            };
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
                tracing::debug!(
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
                    tracing::debug!(
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
                    tracing::warn!(
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

        // Stable per-reader cache identity (issue #1567): hash the immutable
        // file path + generation. Authoritative reader identity, never byte
        // content — combined with a per-site salt to form the cache key.
        let chunk_cache_id = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            path.hash(&mut h);
            generation.hash(&mut h);
            h.finish()
        };

        // Global key→partition-offset cache handle (issue #2059): the process-global
        // shared instance when block caching is enabled, else a per-reader disabled
        // no-op. Built from the open-time config BEFORE `open_config` is moved.
        let key_offset_cache = super::build_key_offset_cache(&open_config);

        // This reader's authoritative inode-stable generation identity (issue #2059),
        // the namespacing half of every global key-cache entry. Resolved ONCE here
        // from the Data.db path + parsed generation, and stored immutably — it stays
        // stable across a #2383 rebind (a path swap over a byte-identical generation),
        // so cached locations survive a rebind. `None` on a stat failure → the cache
        // is bypassed rather than fabricating an identity (no-heuristics #28).
        let generation_identity =
            crate::storage::cache::GenerationIdentity::resolve(path, generation);

        // Cache the immutable `[first_key, last_key]` endpoint tokens ONCE at open
        // (issue #1576, Epic C/C5 perf finding) so `partition_key_out_of_range`
        // only hashes the QUERY key on the hot point-read path instead of
        // re-hashing the two fixed endpoints on every read. Armed only when
        // Summary.db is present with two non-empty endpoints — the exact condition
        // the short-circuit itself requires. Uses the same Cassandra Murmur3 token
        // as the hot path, so the cached values are byte-identical.
        let endpoint_tokens = summary_reader.as_ref().and_then(|s| {
            let first = s.get_first_key();
            let last = s.get_last_key();
            (!first.is_empty() && !last.is_empty()).then(|| {
                (
                    crate::util::cassandra_murmur3::cassandra_murmur3_token(first),
                    crate::util::cassandra_murmur3::cassandra_murmur3_token(last),
                )
            })
        });

        Ok(Self {
            file_path: arc_swap::ArcSwap::from_pointee(path.to_path_buf()),
            file,
            scan_source,
            point_source,
            scan_positional_source,
            header,
            parser,
            index,
            bloom_filter,
            compression_reader,
            config: reader_config,
            open_config,
            platform,
            stats,
            #[cfg(feature = "tombstones")]
            tombstone_merger: TombstoneMerger::new(),
            generation,
            actual_header_size: header_size,
            index_reader,
            index_present_but_unloadable,
            summary_reader,
            endpoint_tokens,
            statistics_reader,
            schema_registry: None, // Will be set by set_schema_registry() after construction
            schema,
            #[cfg(feature = "state_machine")]
            registry_schema: None, // Resolved by resolve_registry_schema() at wiring time (#1692)
            udt_registry: None, // Will be set when available for UDT-aware parsing
            scan_cancel: crate::storage::scan_cancel::ScanCancel::default(), // #2264: set via set_scan_cancel
            compression_info: compression_info.map(Arc::new),
            crc_reader,
            verified_uncompressed_chunks: std::sync::Mutex::new(std::collections::HashSet::new()),
            version_gates,
            bti_partitions_db,
            bti_rows_db,
            chunk_cache,
            chunk_cache_id,
            bti_lookup_memo: std::sync::Mutex::new(None),
            key_offset_cache,
            generation_identity,
        })
    }

    /// Whether this reader's block source is backed by a memory map.
    ///
    /// Test-only hook used to verify that the `use_mmap` config / env wiring
    /// actually promotes an explicit `Buffered` request to mmap end-to-end.
    #[cfg(test)]
    pub(crate) async fn is_mmap_backed(&self) -> bool {
        self.file.lock().await.is_mmap()
    }

    /// Clone the reader's shared positional point source (issue #1573 convoy
    /// scenario). Test-only: lets a test wrap the real source in a slow/serializing
    /// decorator, then reinstall it via [`set_point_source`](Self::set_point_source).
    #[cfg(test)]
    pub(crate) fn clone_point_source(&self) -> Arc<dyn read_at::ReadAt> {
        self.point_source.clone()
    }

    /// Replace the reader's point source (issue #1573 convoy scenario). Test-only;
    /// requires `&mut self`, so it must be called BEFORE the reader is shared
    /// behind an `Arc` across the concurrent point reads under test.
    #[cfg(test)]
    pub(crate) fn set_point_source(&mut self, src: Arc<dyn read_at::ReadAt>) {
        self.point_source = src;
    }

    /// Clone the reader's scan-side positional source (issue #2876). Test-only:
    /// mirrors [`clone_point_source`](Self::clone_point_source) for the sibling
    /// scan-side plane.
    ///
    /// Sole callers are the write-support+lz4-gated Summary-guided scan-plane
    /// regressions in `read_at_point_tests.rs`, so this is gated identically —
    /// under the minimal-build feature set (no `write-support`) those tests do not
    /// exist and this method would otherwise be flagged dead code under `-D warnings`.
    #[cfg(all(test, feature = "write-support", feature = "lz4"))]
    pub(crate) fn clone_scan_positional_source(&self) -> Arc<dyn read_at::ReadAt> {
        self.scan_positional_source.clone()
    }

    /// Replace the reader's scan-side positional source (issue #2876). Test-only;
    /// mirrors [`set_point_source`](Self::set_point_source) — requires `&mut self`,
    /// so call it BEFORE the reader is shared behind an `Arc`. Gated identically to
    /// [`clone_scan_positional_source`](Self::clone_scan_positional_source).
    #[cfg(all(test, feature = "write-support", feature = "lz4"))]
    pub(crate) fn set_scan_positional_source(&mut self, src: Arc<dyn read_at::ReadAt>) {
        self.scan_positional_source = src;
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
    async fn open_buffered_sources(
        path: &Path,
        file_size: u64,
    ) -> Result<(BlockSource, ScanSource)> {
        // A5 read-work counter (FILE_OPENS; consumer C2): one per open(2) that mints
        // a reader fd — here the buffered cold-open / graceful-fallback site. No-op
        // in release (design.md Decision 1/2).
        crate::storage::sstable::read_work_counters::record_file_open();
        Ok((
            BlockSource::buffered_sized(File::open(path).await?, file_size),
            ScanSource::Buffered {
                file_len: file_size,
            },
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
        // Otherwise clamp the configured window so a sub-chunk
        // `direct_io_prefetch_bytes` cannot make one compression chunk straddle
        // many aligned refills (issue #1596, F6). The actual per-SSTable chunk
        // length is not parsed yet at this open-time construction site, so the
        // clamp floors against Cassandra's default 64 KiB chunk — which fully
        // protects the default case (and the 1 MiB default window is already
        // above the floor, so the clamp is a no-op there).
        let direct_window = if matches!(prefetch, PrefetchMode::Off) {
            1
        } else {
            prefetch_window::clamp_direct_prefetch_window(
                prefetch_bytes,
                prefetch_window::DEFAULT_COMPRESSION_CHUNK_BYTES,
            )
        };
        match mode {
            DiskAccessMode::Buffered => Self::open_buffered_sources(path, file_size).await,
            DiskAccessMode::Mmap => match Self::map_file(path) {
                Ok(mmap) => {
                    tracing::debug!(
                        "Opened {} via memory map ({} bytes)",
                        path.display(),
                        file_size
                    );
                    // Best-effort read-ahead advice (Unix-only; madvise has no
                    // Windows equivalent here). Failure is non-fatal.
                    #[cfg(unix)]
                    if let Some(advice) = mmap_advice_for(prefetch) {
                        if let Err(e) = mmap.advise(advice) {
                            tracing::debug!(
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
                    tracing::warn!(
                        "Memory-mapping {} failed ({}); falling back to buffered I/O",
                        path.display(),
                        e
                    );
                    Self::open_buffered_sources(path, file_size).await
                }
            },
            DiskAccessMode::Direct => {
                #[cfg(unix)]
                {
                    match source::DirectCursor::open(path, direct_window) {
                        Ok(cursor) => {
                            tracing::debug!(
                                "Opened {} via direct I/O ({} bytes, {}-byte window)",
                                path.display(),
                                file_size,
                                direct_window
                            );
                            Ok((
                                BlockSource::direct(cursor),
                                ScanSource::Direct {
                                    window: direct_window,
                                    file_len: file_size,
                                },
                            ))
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Direct I/O on {} failed ({}); falling back to buffered I/O",
                                path.display(),
                                e
                            );
                            Self::open_buffered_sources(path, file_size).await
                        }
                    }
                }
                #[cfg(not(unix))]
                {
                    let _ = direct_window;
                    tracing::warn!(
                        "Direct I/O is unavailable on this platform; using buffered I/O for {}",
                        path.display()
                    );
                    Self::open_buffered_sources(path, file_size).await
                }
            }
            // `resolve_disk_access_mode` never yields `Auto`; handle defensively.
            DiskAccessMode::Auto => Self::open_buffered_sources(path, file_size).await,
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
        // A5 read-work counter (FILE_OPENS; consumer C2): one per open(2) that mints
        // a reader fd — here the mmap cold-open site. No-op in release (design.md
        // Decision 1/2).
        crate::storage::sstable::read_work_counters::record_file_open();
        let std_file = std::fs::File::open(path)?;
        // SAFETY: read-only mapping of a file assumed immutable for the
        // reader's lifetime; see the function-level note above.
        let mmap = unsafe { memmap2::MmapOptions::new().map(&std_file)? };
        Ok(mmap)
    }

    /// Choose the mmap the point-read source will use. For a large file
    /// (`file_size >= min_random_bytes`) map a SECOND, dedicated read-only mapping
    /// of the same file and advise it `MADV_RANDOM`, returning that distinct
    /// mapping so scattered point faults read one page instead of the ~128 KiB
    /// read-ahead window (issue #2210). The returned mapping is a SEPARATE allocation
    /// from `scan_mmap`, which THIS function never advises (only `build_block_sources`
    /// does, and only for an explicit `Sequential`/`WillNeed`), so advising the point
    /// map cannot affect the scan map (#1143 preserved). Below the threshold, or if
    /// the dedicated map / its advice fails, share `scan_mmap` unchanged (never keep a
    /// redundant 2nd map). Mapped directly (not via `map_file`): FILE_OPENS untouched.
    #[cfg(unix)]
    fn point_read_mmap(
        path: &Path,
        file_size: u64,
        scan_mmap: &Arc<memmap2::Mmap>,
        min_random_bytes: u64,
    ) -> Arc<memmap2::Mmap> {
        if file_size >= min_random_bytes {
            if let Ok(std_file) = std::fs::File::open(path) {
                // SAFETY: read-only mapping of a file assumed immutable for the
                // reader's lifetime (same contract as `map_file`).
                match unsafe { memmap2::MmapOptions::new().map(&std_file) } {
                    Ok(point_mmap) => match point_mmap.advise(memmap2::Advice::Random) {
                        Ok(()) => {
                            tracing::debug!(
                                "Dedicated MADV_RANDOM point-read mapping for {} ({} bytes, #2210)",
                                path.display(),
                                file_size
                            );
                            return Arc::new(point_mmap);
                        }
                        Err(e) => tracing::debug!(
                            "madvise(RANDOM) on dedicated point map for {} failed ({}); \
                             sharing scan mapping",
                            path.display(),
                            e
                        ),
                    },
                    Err(e) => tracing::debug!(
                        "Dedicated point map for {} failed ({}); sharing scan mapping",
                        path.display(),
                        e
                    ),
                }
            }
        }
        scan_mmap.clone()
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
                tracing::debug!(
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
    ///   owner-pinned **warn-and-proceed** decision (design D4): a `tracing::warn!` is
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
            tracing::warn!(
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
        tracing::debug!(
            "Loaded CRC.db for {}: chunk_size={}, chunks={}",
            path.display(),
            crc.chunk_size(),
            crc.chunk_count()
        );
        Ok(Some(Arc::new(crc)))
    }

    /// Set the schema registry for schema-driven operations.
    ///
    /// This is a SYNC method (`&mut self`, non-async), so it CANNOT await the
    /// async registry to pre-resolve the sync schema-fallback cache
    /// ([`registry_schema`](Self::registry_schema)) — doing so would require a
    /// `block_on`, which #1692 (AG3) forbids on a tokio worker thread. It
    /// therefore only stores the registry and INVALIDATES any previously
    /// pre-resolved cache (the new registry may resolve a different schema).
    ///
    /// Direct callers that intend to trigger a SYNC parse (whose schema-fallback
    /// tier reads `registry_schema`) must, from an async context, either call the
    /// combined [`attach_schema_registry`](Self::attach_schema_registry) instead,
    /// or call [`resolve_registry_schema`](Self::resolve_registry_schema) after
    /// this. Otherwise the sync path deliberately falls through to
    /// header-column construction (or `None`). The crate's own async wiring path
    /// (`open_reader_with_schema`) already does this.
    #[cfg(feature = "state_machine")]
    #[deprecated(
        note = "attaches the registry but CANNOT pre-resolve the sync schema-fallback cache \
                (would need a forbidden block_on, issue #1692); registry schemas will not be \
                available to a subsequent sync `get_table_schema`. Use the async \
                `attach_schema_registry` (which pre-resolves), or call `resolve_registry_schema` \
                after this from an async context, for registry-schema-aware reads."
    )]
    pub fn set_schema_registry(
        &mut self,
        schema_registry: Arc<tokio::sync::RwLock<crate::schema::SchemaRegistry>>,
    ) {
        self.schema_registry = Some(schema_registry);
        // Invalidate the sync fallback cache: a prior registry (if any) may have
        // resolved a schema that no longer applies. It is re-populated only by an
        // explicit `resolve_registry_schema()` / `attach_schema_registry()`.
        self.registry_schema = None;
        tracing::debug!(
            "Schema registry set for {}.{} - enabling schema-driven digest computation",
            self.header.keyspace,
            self.header.table_name
        );
    }

    /// Attach a schema registry AND pre-resolve it into the sync fallback cache
    /// (issue #1692). Async wiring convenience for DIRECT callers: it combines
    /// [`set_schema_registry`](Self::set_schema_registry) with
    /// [`resolve_registry_schema`](Self::resolve_registry_schema) so the sync
    /// `get_table_schema` fallback tier is populated without any `block_on`.
    ///
    /// Prefer this over the bare sync `set_schema_registry` whenever you attach a
    /// registry to a reader you will parse synchronously.
    #[cfg(feature = "state_machine")]
    pub async fn attach_schema_registry(
        &mut self,
        schema_registry: Arc<tokio::sync::RwLock<crate::schema::SchemaRegistry>>,
    ) {
        // Intentional internal use of the sync attach step; we immediately
        // pre-resolve below, which is exactly what the deprecation directs.
        #[allow(deprecated)]
        self.set_schema_registry(schema_registry);
        self.resolve_registry_schema().await;
    }

    /// Set the schema registry for schema-driven operations (non-state_machine builds)
    #[cfg(not(feature = "state_machine"))]
    pub fn set_schema_registry(&mut self, schema_registry: Arc<crate::schema::SchemaRegistry>) {
        self.schema_registry = Some(schema_registry);
        tracing::debug!(
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
        tracing::debug!(
            "UDT registry set for {}.{} - enabling UDT-aware collection parsing",
            self.header.keyspace,
            self.header.table_name
        );
    }

    /// Whether a UDT registry has been wired onto this reader (issue #2310, WS1
    /// #2345). A warm-handle cache that hands SHARED `Arc<SSTableReader>`s to the
    /// reader-based k-way merge seam (`KWayMerger::new_from_readers`, which takes
    /// NO `udt_registry` parameter) MUST open each reader WITH its registry
    /// already resolved before wrapping it in `Arc`; this getter lets that caller
    /// PROVE the registry is present rather than silently decoding a frozen/nested
    /// UDT cell as `Blob` (the #1234 data-loss class).
    pub fn has_udt_registry(&self) -> bool {
        self.udt_registry.is_some()
    }

    /// The `Data.db` path this reader was opened from (issue #2310). The warm
    /// registry keys parsed state on the file's inode-stable generation identity,
    /// so it needs the backing path to `stat` device+inode without re-listing the
    /// directory.
    pub fn file_path(&self) -> PathBuf {
        // Owned clone (cheap relative to a scan/point-read): the path is now
        // interior-mutable to support [`Self::rebind_path`] (#2383), so we cannot
        // hand out a borrow into the `ArcSwap`.
        self.file_path.load().as_ref().clone()
    }

    /// The `Data.db` size in bytes captured at open. Used by the warm registry's
    /// authoritative rebind identity check (issue #2383): a rebind is accepted
    /// only when the live candidate's `(device, inode)` AND size match this
    /// reader's, else it fails closed to a full re-open.
    pub fn file_size(&self) -> u64 {
        self.stats.file_size
    }

    /// Rebind this reader's lazy-scan path to `new_path` WITHOUT re-opening or
    /// re-parsing (issue #2383, the #2356 "rebind-by-inode" direction).
    ///
    /// The caller MUST have already established that `new_path` is the SAME
    /// on-disk generation as this reader by AUTHORITATIVE identity — matching
    /// `(device, inode)` and size (issue #28 no-heuristics; Cassandra snapshot
    /// files are hardlinks to the immutable SSTable, so a same-inode candidate is
    /// byte-identical). The already-open point/scan handles reference the shared
    /// inode and stay valid across the swap; only [`Self::new_scan_cursor`]'s
    /// per-scan `File::open` reads this path, so pointing it at a LIVE hardlink
    /// restores the #2352 ENOENT protection with zero parse cost.
    ///
    /// ## In-flight isolation invariant (roborev-1654 HIGH, adjudicated)
    ///
    /// Mutating this shared `Arc<SSTableReader>`'s path while a concurrent request
    /// scans it does NOT break the in-flight request's isolation, because the
    /// rebind is byte-transparent and monotone-toward-live:
    /// 1. **Same immutable inode.** The sole caller (`warm::registry::rebind`)
    ///    fires only after `rebuild::rebind_matches` proves
    ///    `(device, inode, generation)` + size equality; every rebind target is a
    ///    hardlink to the SAME immutable SSTable inode, so any path this reader
    ///    carries yields byte-identical data (issue #28 no-heuristics).
    /// 2. **Atomic swap.** `file_path` is an `arc_swap::ArcSwap<PathBuf>`; a
    ///    concurrent `file_path()` sees either the old or the new whole `PathBuf`,
    ///    never a torn value.
    /// 3. **Dead → live only.** A rebind happens only when the current path is
    ///    dead and an identity-matching LIVE hardlink exists, so an in-flight
    ///    request's NEXT `new_scan_cursor` `File::open` is strictly MORE likely to
    ///    succeed after the rebind than before (pre-rebind it would ENOENT).
    /// 4. **No semantics off the path.** No `file_path()` consumer re-derives
    ///    keyspace/table/schema from the path after `open`; the path is used ONLY
    ///    for `File::open` (bytes) and all parsed read state (header, compression
    ///    info, CRC, index, generation) lives on `&self`, fixed at open — the
    ///    swapped path changes which hardlink is read, never what the bytes mean.
    ///    This covers BOTH the `Data.db` path AND the lazy `Index.db` path (below):
    ///    both are same-inode hardlink siblings in the rebound snapshot dir.
    ///
    /// ## Rebinding the lazy `Index.db` path too (issue #2356 roborev)
    ///
    /// Repointing ONLY the `Data.db` path reintroduced the #2352 ENOENT class for
    /// the dominant point-read shape: a BIG reader opened lazily over a usable
    /// `Summary.db` (#2412 §A) keeps its own `Index.db` path, and every DEFERRED
    /// `Index.db` open (`ensure_materialized`, the Summary-guided bounded-interval
    /// point probe) reads THAT path. If the original snapshot dir is torn down
    /// before the reader ever materialized, a not-yet-materialized reader would
    /// `File::open` the dead path and ENOENT. So the rebind ALSO repoints the
    /// `IndexReader`'s path to the `Index.db` hardlink sibling of `new_path`,
    /// keeping every `Index.db` consumer (deferred-materialize, point-interval, and
    /// the streaming walk's `current_index_db_path` Data.db-sibling derivation) on
    /// the live generation.
    pub fn rebind_path(&self, new_path: &Path) {
        self.file_path.store(Arc::new(new_path.to_path_buf()));
        // Follow the rebind for the lazy Index.db path too: derive the Index.db
        // hardlink sibling of the new Data.db path and repoint the IndexReader, so a
        // deferred materialize / point-interval read opens the live file, not the
        // dead open-time snapshot path (issue #2356 roborev, #2352 class).
        if let Some(index_reader) = self.index_reader.as_ref() {
            if let Some(index_sibling) = index_db_sibling(new_path) {
                index_reader.rebind_path(&index_sibling);
            }
        }
    }

    /// The immutable `[first_key_token, last_key_token]` endpoints cached at open
    /// (issue #1576), or `None` when `Summary.db` was absent/empty. The Flight
    /// warm registry (issue #2310) uses this to token-prune a WARM reader set with
    /// ZERO extra I/O — a warm hit re-reads no `Summary.db`, preserving the
    /// "zero Index/Summary/Statistics/bloom parse" property even for a
    /// token-filtered scan. SSTables store partitions in token order, so the
    /// first endpoint is the min token and the last the max.
    pub fn endpoint_tokens(&self) -> Option<(i64, i64)> {
        self.endpoint_tokens
    }

    /// Whether this reader's `Index.db` partition map is CURRENTLY fully
    /// resident (issue #2412 §D — the Flight warm registry's memory accounting).
    ///
    /// A BIG reader opened lazily over a usable `Summary.db` (design §A) reports
    /// `false` until some consumer's [`ensure_materialized`]-driven full parse
    /// (a full/compaction scan whose Summary-guided streaming walk `FellBack`)
    /// actually happens; the Summary-guided point/scan paths (§B/§C) never
    /// trigger it. An eagerly-opened reader (the `Summary.db`-absent FellBack
    /// case, §A1) reports `true` immediately — its `Index.db` was fully parsed
    /// at open. No `Index.db` at all (absent component) reports `true` (there is
    /// no resident cost to represent either way).
    ///
    /// [`ensure_materialized`]: crate::storage::sstable::index_reader::IndexReader
    pub fn index_is_materialized(&self) -> bool {
        self.index_reader
            .as_ref()
            .map(|ir| ir.is_materialized())
            .unwrap_or(true)
    }

    /// Wire a cooperative-cancellation token into this reader's long-running
    /// scans (issue #2264).
    ///
    /// The compaction streaming read and its sequential-scan fallback poll this
    /// token at a bounded interval, so a cancelled Flight `do_get` abandons an
    /// otherwise uninterruptible full-Data.db walk within milliseconds instead of
    /// waiting out the coarse ~1–2 min transport backstop. Idempotent; the last
    /// token set wins. Readers never wired keep the default never-cancel flag.
    pub fn set_scan_cancel(&mut self, cancel: crate::storage::scan_cancel::ScanCancel) {
        self.scan_cancel = cancel;
    }

    /// Get reader statistics
    pub async fn stats(&self) -> Result<&SSTableReaderStats> {
        Ok(&self.stats)
    }

    /// Close the reader and release resources
    pub async fn close(self) -> Result<()> {
        debug!("Closing SSTable reader for {:?}", self.file_path());
        // File will be closed automatically when dropped. The shared B1
        // decompressed-chunk cache is reference-counted and outlives one reader
        // (issue #1568: the per-reader block cache that was cleared here is gone).
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
        let file_path = self.file_path();
        let filename = file_path
            .file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| {
                Error::InvalidPath(format!("Invalid SSTable filename: {:?}", file_path))
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
        use tracing::warn;

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
            .field("file_path", &self.file_path())
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
