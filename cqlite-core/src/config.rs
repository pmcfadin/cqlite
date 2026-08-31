//! Configuration management for CQLite

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Main configuration structure for CQLite database.
///
/// # Every knob in #1696's CENSUS is read or deleted — and the exception (#1696 roborev r5 F3)
///
/// Scoped to the census deliberately, because the unqualified version ("every
/// field here is read by something") is contradicted by our own standing guard,
/// `cqlite-core/tests/config_knob_behavior_guard.rs`, which records every
/// [`CompressionConfig`] field as DECORATIVE with zero production readers.
///
/// What #1696 (epic #1685, "config honesty") examined, it either kept because
/// something reads it, or DELETED: `storage`'s `max_sstable_size` /
/// `block_size` / `enable_bloom_filters` / `bloom_filter_fp_rate` /
/// `io_threads` / `sync_mode`, `query`'s `plan_cache_size` /
/// `enable_optimization` / `parallel`, and the entire `performance` tree are
/// gone — setting any of them changed nothing, silently.
///
/// The KNOWN exception is [`CompressionConfig`] (`enabled` / `algorithm` /
/// `level` / `min_block_size`). Those four were NOT in #1696's census and are
/// deliberately left in place: the read path takes its algorithm from
/// `CompressionInfo.db` as the no-heuristics mandate requires, and the write
/// surface is uncompressed-only (**#1406** owns that boundary and the
/// compressed-write wiring), so there is nothing for them to steer today. No dedicated
/// removal issue exists; they belong to the open epic **#1685**. The guard is the
/// authority on which fields are decorative — read it, do not read a claim of
/// universal coverage into this heading.
///
/// Deleting a field is deliberately a COMPILE error for an embedder writing
/// Rust: that is the loudest signal available, and it is preferred over a field
/// that keeps deserializing while doing nothing.
///
/// # But this is ALSO a deserialization surface (#1696 roborev F1)
///
/// `Config` derives `Deserialize`, and serde DISCARDS unknown fields — so a
/// caller who configures CQLite through JSON or a dict (the Python bindings'
/// bridge) gets no compile step and, before #1696's F1 fix, no signal at all: a
/// pre-change document naming a deleted knob loaded successfully and was
/// silently ignored. The rule is stated at the layer where a knob is SET, so the
/// authoring surfaces report removed keys by name instead:
/// [`Self::from_json_str`] / [`Self::from_json_str_reporting_removed`] for this
/// crate's JSON surface (see [`crate::config_removed_keys`]), and
/// `cqlite_cli::config::removed_keys` for the CLI's file surface. Both use the
/// same posture — parse-and-ignore PLUS a named warning, never
/// `deny_unknown_fields` — because ONE posture crate-wide is the requirement,
/// and hard-failing would leave an existing caller with no migration path over
/// keys that never did anything.
///
/// # ENFORCED where, exactly — and the ONE surface that is not (#3520)
///
/// Those constructors are OPTIONAL, so they do not cover the serde boundary
/// itself: `serde_json::from_str::<Config>` / `from_value::<Config>` bypass them
/// and still DISCARD removed keys in SILENCE. Enforced surfaces are the CLI
/// config-file loader, the Python bindings entry points, and Rust field access (a
/// compile error, for Rust callers only). The unenforced one is a direct serde
/// deserialization by an embedder — **issue #3520**, scoped out of #1696
/// deliberately (roborev r2 F3) and pinned by
/// `direct_serde_deserialization_is_the_unreported_surface`. Nothing here should
/// be read as universal coverage.
///
/// The standing guard is `cqlite-core/tests/config_knob_behavior_guard.rs`:
/// every leaf field below must be registered there with either a set-knob →
/// assert-observable-difference test or an explicit reason why no observable
/// difference is expressible. A newly added `pub` field with neither FAILS that
/// test — which is the point, since "nobody asked whether this knob is read" is
/// how the removed ones accumulated.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    /// Storage engine configuration
    pub storage: StorageConfig,

    /// Memory management configuration
    pub memory: MemoryConfig,

    /// Query engine configuration
    pub query: QueryConfig,

    /// WASM-specific configuration
    #[cfg(target_arch = "wasm32")]
    pub wasm: WasmConfig,
}

/// Storage engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// MemTable size threshold for flushing, in bytes (default: 64MB).
    ///
    /// This is the AUTHORITATIVE flush trigger for the write path: it is the
    /// single value `WriteEngineConfig::from_config` translates into
    /// `WriteEngineConfig::memtable_flush_threshold` (issue #1697).
    ///
    /// The default changed 16MB -> 64MB in #1697: before that fix this field had
    /// no production reader — the engine carried its own private 64MB default,
    /// so 64MB is the value that always actually ran. Keeping the RUNNING value
    /// preserves behaviour; adopting the decorative 16MB would have silently
    /// quadrupled everyone's flush rate.
    pub memtable_size_threshold: u64,

    /// MemTable HARD limit in bytes (default: 256MB) — the admission ceiling.
    ///
    /// Live knob: the write engine's `check_admission` REJECTS a write whose
    /// mutation exceeds this on its own, or that would push the memtable over
    /// it. Before issue #1697 it existed only as the private
    /// `WriteEngineConfig::DEFAULT_HARD_LIMIT`, so an embedder could be
    /// hard-failed by a ceiling they had no way to see or change. The default is
    /// unchanged (256MB): this exposes the knob, it does not alter behaviour.
    /// [`Config::validate`] requires it to be STRICTLY GREATER than
    /// [`Self::memtable_size_threshold`], since a ceiling at or below the flush
    /// threshold wedges the engine — writes are rejected before a flush can ever
    /// relieve the memtable, and with zero headroom an ordinary write does it —
    /// and requires BOTH knobs to fit in the target's `usize` (see `validate`;
    /// only reachable on 32-bit/wasm32). Note that headroom alone is not a
    /// wedge-freedom guarantee: a single mutation larger than the headroom still
    /// wedges, which is an admission-side defect tracked as #3404.
    #[serde(default = "default_memtable_hard_limit")]
    pub memtable_hard_limit: u64,

    /// Compaction configuration
    pub compaction: CompactionConfig,

    /// Compression configuration
    pub compression: CompressionConfig,

    /// Legacy promote-only flag: it upgrades an **explicit**
    /// [`DiskAccessMode::Buffered`] request to [`DiskAccessMode::Mmap`].
    ///
    /// It does **not** select the backend — [`Self::disk_access_mode`] does, and its
    /// `Auto` default already memory-maps most Data.db files (see that field). So
    /// `false` does not mean "buffered I/O", and `true` changes nothing unless
    /// something explicitly requested `Buffered`. A mapped file is served from the
    /// page cache with no per-block `read` syscall, as Cassandra's mmap mode does.
    ///
    /// # Safety / platform constraints
    ///
    /// A memory map aliases the file's bytes for the reader's lifetime. Only
    /// enable this when the SSTables are **immutable local files**:
    /// - Mutating, truncating, or deleting a mapped file out from under a live
    ///   reader is undefined behaviour and can raise `SIGBUS`, terminating the
    ///   process. CQLite never rewrites its own mapped inputs, but external
    ///   tools must not either.
    /// - Network and overlay filesystems (NFS, SMB, FUSE, some container
    ///   overlays) can fault mid-read after a successful map; prefer buffered
    ///   I/O there.
    ///
    /// # Interaction with the write engine (Issue #591)
    ///
    /// This setting only affects the read path. Compaction's input readers force
    /// `use_mmap = false` + explicit `Buffered` (only `CQLITE_USE_MMAP=1` promotes even
    /// those); each input is unpublished by removing its `TOC.txt` before the data
    /// components, best-effort. So enabling mmap for queries is safe
    /// alongside background compaction: a compaction never holds a mapping over a
    /// file it then deletes, and on Windows a data file still pinned by a mapped
    /// reader becomes an invisible orphan (reclaimed on the next startup) rather
    /// than a failed delete or a source of duplicate rows.
    ///
    /// Can also be enabled at runtime by setting `CQLITE_USE_MMAP=1`.
    ///
    /// `#[serde(default)]` keeps configs serialized before this field existed
    /// (which omit it) deserializing successfully, defaulting to no promotion.
    #[serde(default = "default_use_mmap")]
    pub use_mmap: bool,

    /// Minimum Data.db size (bytes) at which [`DiskAccessMode::Auto`] maps. Default 4096.
    ///
    /// It gates ONLY `Auto`, which uses buffered I/O below it (a tiny file does not
    /// repay the mapping setup); an explicit `Mmap` — including a `Buffered` promoted
    /// by [`Self::use_mmap`] — is not size-gated, only a zero-length file falls back.
    ///
    /// `#[serde(default)]` for backward compatibility with older payloads.
    #[serde(default = "default_mmap_min_size_bytes")]
    pub mmap_min_size_bytes: usize,

    /// How the SSTable read path accesses Data.db on disk.
    ///
    /// Defaults to [`DiskAccessMode::Auto`], which sizes each Data.db file
    /// against system RAM and picks the backend automatically:
    /// - files below [`Self::mmap_min_size_bytes`] use buffered I/O (mapping a
    ///   tiny file is not worth the setup cost);
    /// - files up to [`Self::direct_io_memory_fraction`] of system memory are
    ///   **memory-mapped**, so repeated scans stay resident in the page cache;
    /// - files larger than that fraction use **direct I/O** (`O_DIRECT` on
    ///   Linux, `F_NOCACHE` on macOS), which bypasses the page cache so a
    ///   single huge scan does not evict everything else the host has cached.
    ///
    /// Set an explicit [`DiskAccessMode::Buffered`], [`DiskAccessMode::Mmap`],
    /// or [`DiskAccessMode::Direct`] to override the heuristic. The legacy
    /// [`Self::use_mmap`] flag only PROMOTES an explicit `Buffered` request to
    /// `Mmap`; it never changes what `Auto` resolves to.
    ///
    /// Can also be set at runtime via `CQLITE_DISK_ACCESS_MODE`
    /// (`auto` / `buffered` / `mmap` / `direct`).
    #[serde(default)]
    pub disk_access_mode: DiskAccessMode,

    /// Fraction of total system memory above which [`DiskAccessMode::Auto`]
    /// switches a file from memory-mapped to direct I/O. Defaults to `0.5`
    /// (half of RAM). Ignored when system memory cannot be determined (in which
    /// case `Auto` never escalates to direct I/O).
    ///
    /// The legal range is `(0.0, 1.0]` and [`Config::validate`] REJECTS anything
    /// outside it, NaN and the infinities included (issue #1696). It used to be
    /// silently clamped instead — a `2.0` or a `-1` quietly became the `0.5`
    /// default — so the value an operator set was not the value that ran. It is a
    /// FRACTION, never a byte count; to always bypass the page cache, ask for
    /// [`DiskAccessMode::Direct`].
    #[serde(default = "default_direct_io_memory_fraction")]
    pub direct_io_memory_fraction: f64,

    /// Read-ahead / prefetch strategy applied to the chosen backend.
    ///
    /// Defaults to [`PrefetchMode::Auto`], which issues **no** mmap `madvise`
    /// (relying on the kernel's default read-ahead) and only enables the
    /// direct-I/O prefetch window of [`Self::direct_io_prefetch_bytes`]. Set
    /// [`PrefetchMode::Off`] to disable explicit hints (relying only on default
    /// kernel read-ahead / single-block direct reads). Can also be set via
    /// `CQLITE_PREFETCH` (`off` / `sequential` / `willneed` / `auto`).
    #[serde(default)]
    pub prefetch: PrefetchMode,

    /// Size in bytes of the read-ahead window used by the direct-I/O backend, and by
    /// nothing else: the buffered backend ignores it (`open_buffered_sources` takes no
    /// prefetch bytes; its `BufReader::new` capacity is tokio's 8 KiB default). Rounded
    /// up to the I/O alignment; 1 MiB default; inert while `prefetch` is `Off`.
    #[serde(default = "default_direct_io_prefetch_bytes")]
    pub direct_io_prefetch_bytes: usize,
}

/// Selects which backend the SSTable read path uses for Data.db I/O.
///
/// See [`StorageConfig::disk_access_mode`] for the per-variant semantics and
/// the [`DiskAccessMode::Auto`] sizing heuristic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DiskAccessMode {
    /// Size each file against system RAM and pick buffered / mmap / direct.
    #[default]
    Auto,
    /// Always use buffered file I/O through the OS page cache.
    Buffered,
    /// Always memory-map the file. Unlike the [`DiskAccessMode::Auto`] heuristic,
    /// this honors the user's explicit request and is **not** gated by
    /// [`StorageConfig::mmap_min_size_bytes`] (the size threshold only steers
    /// `Auto`); a zero-length file still falls back to buffered I/O since an
    /// empty map is invalid.
    Mmap,
    /// Always use direct I/O, bypassing the OS page cache.
    Direct,
}

/// Selects the read-ahead hint applied to the active disk-access backend.
///
/// See [`StorageConfig::prefetch`]. `Sequential` / `WillNeed` map to the
/// corresponding `madvise(2)` advice on the mmap backend; on the direct-I/O
/// backend any non-`Off` value enables the [`StorageConfig::direct_io_prefetch_bytes`]
/// read-ahead window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PrefetchMode {
    /// No explicit prefetch hint; rely on default kernel behaviour.
    Off,
    /// Hint sequential access (aggressive read-ahead, drop-behind).
    Sequential,
    /// Hint that the mapped/region bytes will be needed soon (eager fault-in).
    WillNeed,
    /// Let the backend choose. For mmap this issues **no** madvise and relies on
    /// the kernel's default read-ahead: `MADV_SEQUENTIAL`'s drop-behind evicts
    /// hot pages under concurrent write load and inflates the read-side p99 tail
    /// (issue #1143), so `Auto` avoids it while keeping the isolated mmap win.
    /// For direct I/O it enables the windowed read-ahead
    /// ([`StorageConfig::direct_io_prefetch_bytes`]). Request
    /// [`PrefetchMode::Sequential`] explicitly for `MADV_SEQUENTIAL` behaviour.
    #[default]
    Auto,
}

/// Default for [`StorageConfig::use_mmap`]: no promotion (see the field doc).
fn default_use_mmap() -> bool {
    false
}

/// Default for [`StorageConfig::memtable_hard_limit`]: 256MB, the value the
/// write engine always used privately (issue #1697).
fn default_memtable_hard_limit() -> u64 {
    256 * 1024 * 1024
}

/// Default for [`StorageConfig::mmap_min_size_bytes`]: one page.
fn default_mmap_min_size_bytes() -> usize {
    4096
}

/// Default for [`StorageConfig::direct_io_memory_fraction`]: half of RAM.
fn default_direct_io_memory_fraction() -> f64 {
    0.5
}

/// Default for [`StorageConfig::direct_io_prefetch_bytes`]: 1 MiB.
fn default_direct_io_prefetch_bytes() -> usize {
    1024 * 1024
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            // 64MB / 256MB: the values the write engine always used (#1697).
            // Shared with the serde defaults so the two can never drift.
            memtable_size_threshold: 64 * 1024 * 1024,
            memtable_hard_limit: default_memtable_hard_limit(),
            compaction: CompactionConfig::default(),
            compression: CompressionConfig::default(),
            // Opt-in; buffered I/O is the portable, safe default. Shared with
            // the serde defaults so the two can never drift.
            use_mmap: default_use_mmap(),
            mmap_min_size_bytes: default_mmap_min_size_bytes(),
            disk_access_mode: DiskAccessMode::default(),
            direct_io_memory_fraction: default_direct_io_memory_fraction(),
            prefetch: PrefetchMode::default(),
            direct_io_prefetch_bytes: default_direct_io_prefetch_bytes(),
        }
    }
}

/// Compaction strategy configuration — the authoritative source for the write
/// path's Size-Tiered Compaction Strategy (STCS), consumed via
/// `WriteEngineConfig::from_config` (issues #1619, #1697). Decorative
/// `strategy`/`max_sstables`/`size_ratio`/`max_threads`/`background_interval`
/// knobs, read by no behavior, were removed in #1619 rather than left in place.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Enable automatic (STCS) compaction. When `false`, the write engine
    /// installs no merge policy and `maintenance_step` is a no-op.
    pub auto_compaction: bool,

    /// STCS `min_threshold`: minimum number of SSTables in a size bucket before
    /// a compaction is triggered (default: 4). Ignored when
    /// [`Self::auto_compaction`] is `false`. Wired to the write engine by
    /// `WriteEngineConfig::from_config` (issue #1697).
    #[serde(default = "default_compaction_min_threshold")]
    pub min_threshold: usize,

    /// STCS `max_threshold`: maximum number of SSTables merged together in one
    /// compaction step (default: 32). Ignored when [`Self::auto_compaction`] is
    /// `false`. Wired to the write engine by `WriteEngineConfig::from_config`
    /// (issue #1697).
    #[serde(default = "default_compaction_max_threshold")]
    pub max_threshold: usize,
}

/// Default for [`CompactionConfig::min_threshold`]: Cassandra's STCS default.
fn default_compaction_min_threshold() -> usize {
    4
}

/// Default for [`CompactionConfig::max_threshold`]: Cassandra's STCS default.
fn default_compaction_max_threshold() -> usize {
    32
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            auto_compaction: true,
            // Shared with the serde defaults so the two can never drift.
            min_threshold: default_compaction_min_threshold(),
            max_threshold: default_compaction_max_threshold(),
        }
    }
}

/// Memory management configuration.
///
/// Collapsed to exactly one real caching knob (issue #1568, Epic B/B2): the
/// block/chunk-cache byte budget (`block_cache.max_size`), wired as the B1
/// [`DecompressedChunkCache`](crate::storage::cache::DecompressedChunkCache)
/// capacity. The former decorative `row_cache` / `query_cache` / `allocator`
/// knobs (wired to nothing at runtime) were deleted. `deny_unknown_fields`
/// makes a config that still names a removed knob **fail closed** on
/// deserialization rather than silently ignoring it (which would suggest the
/// removed knob still has effect).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryConfig {
    /// Maximum total memory usage (default: 1GB)
    pub max_memory: u64,

    /// Block/chunk cache configuration. `block_cache.max_size` is the real,
    /// wired byte budget of the shared decompressed-chunk cache.
    pub block_cache: CacheConfig,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        let max_memory = 1024 * 1024 * 1024; // 1GB

        Self {
            max_memory,
            block_cache: CacheConfig {
                enabled: true,
                max_size: max_memory / 4, // 256MB
                policy: CachePolicy::Lru,
            },
        }
    }
}

/// Cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Enable this cache
    pub enabled: bool,

    /// Maximum cache size in bytes
    pub max_size: u64,

    /// Cache eviction policy
    pub policy: CachePolicy,
}

/// Cache eviction policy.
///
/// The shared decompressed-chunk cache is LRU (issue #1567/#1568). The
/// never-selected `Lfu` / `Arc` variants were removed (Epic B/B2); a config
/// naming them now fails to deserialize (unknown variant) rather than silently
/// mapping to a default.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CachePolicy {
    /// Least Recently Used
    Lru,
}

/// Default byte ceiling for a materialized SELECT result set (issue #1582).
///
/// 64 MiB. See [`QueryConfig::max_result_bytes`] for the derivation from the
/// project's <128MB process memory target.
pub const DEFAULT_MAX_RESULT_BYTES: u64 = 64 * 1024 * 1024;

/// Serde default for [`QueryConfig::max_result_bytes`] (issue #1582).
///
/// Backward-compat: a `QueryConfig` serialized before this field existed (e.g.
/// a Python JSON/dict config) has no `max_result_bytes` key. Without a serde
/// default, deserialization fails with a missing-field error; with it, such a
/// config takes the shipped [`DEFAULT_MAX_RESULT_BYTES`] budget.
fn default_max_result_bytes() -> u64 {
    DEFAULT_MAX_RESULT_BYTES
}

/// Serde default for [`QueryConfig::max_result_rows`] (issue #1582).
///
/// Backward-compat + robustness: a `QueryConfig` serialized without this key
/// (or a partial JSON/dict config) still deserializes, taking the shipped
/// 1,000,000-row secondary safety valve rather than failing with a missing
/// field. Keeps the knob real (not decorative) and consistent with
/// [`default_max_result_bytes`].
fn default_max_result_rows() -> u64 {
    1_000_000
}

/// Forced SELECT access path (issue #1918).
///
/// A **test/debug** control that removes doubt about which access path serves a
/// `SELECT`. It never changes value decoding, tombstone/timestamp reconciliation,
/// or WRITETIME/TTL semantics — it governs *routing only* — and is chosen
/// exclusively from explicit operator config/env, never inferred from data bytes
/// (no-heuristics mandate). Set programmatically via
/// [`QueryConfig::forced_read_path`] or per-process via the `CQLITE_READ_PATH`
/// environment variable (`auto|point|full`, case-insensitive), with config taking
/// precedence over env. **Not a performance recommendation.**
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ReadPathMode {
    /// Today's behavior: the classifier chooses point-vs-full per query. An unset
    /// knob is byte-for-byte this mode.
    #[default]
    Auto,
    /// Force a genuinely partition-targeted lookup. **Fails closed** with
    /// [`crate::Error::ForcedReadPathUnavailable`] whenever the executor would not
    /// run a partition-targeted lookup — never a silent full scan.
    Point,
    /// Force the full-scan + reconciliation path regardless of classification,
    /// recording [`crate::query::access_path::FallbackReason::ForcedFullScan`].
    Full,
}

/// Query engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Maximum wall-clock budget for ONE query execution (issue #1695).
    ///
    /// ENFORCED, not advisory: every public query entry point on the engine
    /// (`execute`, `execute_streaming`, `execute_with_params`, `execute_prepared`)
    /// runs under a single `tokio::time::timeout` at the engine chokepoint and
    /// fails with [`crate::Error::QueryTimeout`] when the budget elapses.
    ///
    /// **`Duration::ZERO` is the "no timeout" sentinel** — an explicitly LEGAL
    /// value meaning unbounded execution ([`Config::validate`] never rejects it).
    /// There is no `Option` here, so `ZERO` is the only way to disable the bound.
    /// The CLI knob is `performance.query_timeout_ms` (0 ⇒ unbounded).
    ///
    /// For `execute_streaming` the budget covers the whole SETUP future — parse,
    /// plan, stream setup, and (for the plan shapes that materialize before
    /// streaming) the entire scan — but NOT the caller's later row consumption
    /// from the returned iterator; see
    /// [`crate::query::engine::QueryEngine::execute_streaming`] for the exact
    /// scope.
    ///
    /// Default: 300s.
    pub max_execution_time: Duration,

    /// Force the SELECT access-path decision (issue #1918).
    ///
    /// `None` (the default) leaves routing to the per-query classifier and the
    /// `CQLITE_READ_PATH` env knob; `Some(mode)` forces that mode and takes
    /// precedence over the env var. A **test/debug** control — see
    /// [`ReadPathMode`]. `#[serde(default)]` keeps configs serialized before this
    /// field existed deserializing successfully (absent = `None`).
    #[serde(default)]
    pub forced_read_path: Option<ReadPathMode>,

    /// Maximum number of rows to return in a result set.
    ///
    /// A *secondary* safety valve, retained for defense-in-depth (issue #1582).
    /// The primary guard on a materialized result is now `max_result_bytes`: a
    /// row count is the wrong unit because 1M skinny rows can fit comfortably
    /// while 100k wide rows blow the <128MB memory target. Still load-bearing:
    /// the materializing SELECT path enforces this row-count ceiling alongside
    /// the byte budget (lowering it makes a wide-row-count result trip even
    /// under the byte budget), so it is a real knob, not decoration.
    #[serde(default = "default_max_result_rows")]
    pub max_result_rows: u64,

    /// Byte ceiling on a MATERIALIZED result set (issue #1582 / D6).
    ///
    /// While the SELECT executor collects a materialized `Vec<QueryRow>`, it
    /// tracks a running estimate of the result's logical size (via the shared
    /// `crate::memory::estimate_value_size` estimator) and fails with
    /// [`crate::Error::ResultTooLarge`] once this ceiling is crossed — telling
    /// the caller to add a `LIMIT` or use the streaming API. This is the
    /// correct-unit primary guard; `max_result_rows` remains as a secondary
    /// valve. Streaming queries are bounded by their channel buffer, so this
    /// budget does not apply to them.
    ///
    /// Default: [`DEFAULT_MAX_RESULT_BYTES`] (64 MiB). Chosen well below the
    /// project's <128MB process memory target: the estimator measures *logical*
    /// content bytes and does not count per-row container overhead
    /// (`HashMap<Arc<str>, Value>` slots, `String`/`Vec` capacity slack, row
    /// metadata), which in practice roughly doubles real heap use — so a 64 MiB
    /// logical ceiling keeps a fully-materialized result comfortably inside the
    /// process budget while leaving headroom for readers, caches, and decode
    /// buffers.
    #[serde(default = "default_max_result_bytes")]
    pub max_result_bytes: u64,

    /// Query cache size (for plan caching)
    pub query_cache_size: Option<usize>,

    /// Query parallelism thread count
    pub query_parallelism: Option<usize>,

    /// Number of iterations for query analysis
    pub analyze_iterations: Option<usize>,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_execution_time: Duration::from_secs(300), // 5 minutes
            forced_read_path: None,
            max_result_rows: 1_000_000,
            max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
            query_cache_size: Some(100),
            query_parallelism: Some(num_cpus::get()),
            analyze_iterations: Some(5),
        }
    }
}

/// WASM-specific configuration
#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmConfig {
    /// Use IndexedDB for persistent storage
    pub use_indexeddb: bool,

    /// Maximum memory usage in WASM (default: 256MB)
    pub max_memory: u64,

    /// Enable WASM SIMD optimizations
    pub enable_simd: bool,

    /// Enable Web Workers for background tasks
    pub enable_workers: bool,

    /// Maximum number of Web Workers
    pub max_workers: usize,
}

#[cfg(target_arch = "wasm32")]
impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            use_indexeddb: true,
            max_memory: 256 * 1024 * 1024, // 256MB
            enable_simd: true,
            enable_workers: true,
            max_workers: 4,
        }
    }
}

/// Compression algorithms
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// LZ4 compression (fast)
    Lz4,
    /// Snappy compression (balanced)
    Snappy,
    /// Deflate compression (good compression ratio)
    Deflate,
    /// ZSTD compression (high compression ratio)
    Zstd,
}

/// Compression configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// Enable compression
    pub enabled: bool,

    /// Compression algorithm to use
    pub algorithm: CompressionAlgorithm,

    /// Compression level (algorithm-specific)
    pub level: i32,

    /// Minimum block size to compress (smaller blocks are stored uncompressed)
    pub min_block_size: u32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithm: CompressionAlgorithm::Lz4,
            level: 1,             // Fast compression
            min_block_size: 1024, // 1KB minimum
        }
    }
}

impl Config {
    /// Create a configuration optimized for memory usage
    pub fn memory_optimized() -> Self {
        let mut config = Self::default();

        // Reduce memory usage
        config.storage.memtable_size_threshold = 4 * 1024 * 1024; // 4MB
        config.memory.max_memory = 256 * 1024 * 1024; // 256MB
        config.memory.block_cache.max_size = 64 * 1024 * 1024; // 64MB

        // Enable aggressive compression
        config.storage.compression.algorithm = CompressionAlgorithm::Zstd;
        config.storage.compression.enabled = true;

        config
    }

    /// Create a configuration optimized for performance
    pub fn performance_optimized() -> Self {
        let mut config = Self::default();

        // Increase memory usage for better performance
        // Above the 64MB default (#1697 raised the default to the value that
        // always ran), so this preset still trades memory for throughput.
        config.storage.memtable_size_threshold = 128 * 1024 * 1024; // 128MB
        config.memory.max_memory = 4 * 1024 * 1024 * 1024; // 4GB

        // Use faster compression
        config.storage.compression.algorithm = CompressionAlgorithm::Lz4;
        config.storage.compression.enabled = true;

        // More aggressive caching
        config.memory.block_cache.max_size = 1024 * 1024 * 1024; // 1GB

        config
    }

    /// Create a configuration optimized for WASM deployment
    #[cfg(target_arch = "wasm32")]
    pub fn wasm_optimized() -> Self {
        let mut config = Self::memory_optimized();

        // WASM-specific optimizations
        config.wasm.max_memory = 128 * 1024 * 1024; // 128MB
        config.wasm.enable_simd = true;
        config.wasm.enable_workers = false; // Conservative default

        // Reduce overall memory usage for WASM
        config.memory.max_memory = 128 * 1024 * 1024; // 128MB
        config.storage.memtable_size_threshold = 2 * 1024 * 1024; // 2MB

        // Disable background compaction, which may not work well in WASM.
        config.storage.compaction.auto_compaction = false;

        config
    }

    /// Create a test-optimized configuration
    #[cfg(test)]
    pub fn test_config() -> Self {
        let mut config = Config::default();

        // Disable background compaction, which can cause test hangs.
        config.storage.compaction.auto_compaction = false;

        // Reduce timeouts for faster test execution
        config.query.max_execution_time = std::time::Duration::from_secs(1);

        // Smaller memory usage for tests. The cache budget is scaled WITH
        // `max_memory` (the same 1/4 ratio `MemoryConfig::default` uses), not
        // left at the 1GB default's 256MB: `validate` requires
        // `block_cache.max_size <= max_memory`, and a constructor that emits a
        // config its own `validate` rejects is a latent contradiction — it went
        // unnoticed only because nothing on the open path ever validated.
        //
        // This is NO LONGER load-bearing for any open path: `Database::open`
        // enforces the `direct_io_memory_fraction` range alone, not the cache
        // budget (#1696 roborev r3 F3 narrowed it, residual #3525). It is kept
        // because it is correct on its own merits — the fix is to the
        // constructor's self-consistency, not to whoever happens to validate.
        config.memory.max_memory = 64 * 1024 * 1024; // 64MB
        config.memory.block_cache.max_size = config.memory.max_memory / 4; // 16MB
        config.storage.memtable_size_threshold = 1024 * 1024; // 1MB

        config
    }
}

/// JSON deserialization entry points (`Config::from_json_str`), split out under
/// the campsite rule (epic #1116).
#[path = "config_json.rs"]
mod json;

/// `Config::validate` and the rules it enforces, split out under the campsite
/// rule (epic #1116).
#[path = "config_validate.rs"]
mod validate;

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;
