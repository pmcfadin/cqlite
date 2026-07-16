//! Metric catalog — the single source of truth for CQLite metric names + units.
//!
//! Downstream observability issues (epic #1031) MUST import the constants from
//! this module instead of hard-coding metric-name strings. Centralising the
//! names here keeps the emitted telemetry consistent across subsystems and lets
//! us evolve names in exactly one place.
//!
//! # Naming conventions
//!
//! Names follow the OpenTelemetry metric semantic conventions:
//! - dot-separated, lower-snake namespaces, rooted under `cqlite.`
//! - units use the UCUM annotations OTel recommends (`{row}`, `s`, `By`, `1`,
//!   `{error}`, …); see [`unit`].
//! - counters describe a monotonically increasing total; their name reflects the
//!   thing being counted (rows, bytes, errors).
//! - histograms describe a distribution (durations, sizes).
//! - gauges describe a current value (in-flight, cache occupancy).
//!
//! # Attribute cardinality
//!
//! Every metric is documented with the *bounded* attribute set it may carry.
//! NEVER attach unbounded values (raw error messages, partition keys, full
//! queries) as attributes — those explode cardinality and cost. Bounded
//! attribute keys live in [`attr`].

/// Recommended UCUM units for the catalog metrics.
///
/// Use these constants when constructing instruments so the unit string stays
/// consistent with the metric definitions below.
pub mod unit {
    /// Dimensionless count / ratio.
    pub const DIMENSIONLESS: &str = "1";
    /// A count of rows (UCUM annotation).
    pub const ROWS: &str = "{row}";
    /// A count of partitions.
    pub const PARTITIONS: &str = "{partition}";
    /// A count of SSTables.
    pub const SSTABLES: &str = "{sstable}";
    /// A count of errors.
    pub const ERRORS: &str = "{error}";
    /// A count of tombstones.
    pub const TOMBSTONES: &str = "{tombstone}";
    /// Bytes.
    pub const BYTES: &str = "By";
    /// Seconds (OTel prefers base-unit seconds for durations).
    pub const SECONDS: &str = "s";
    /// A count of OS threads (UCUM annotation).
    pub const THREADS: &str = "{thread}";
    /// A count of open file descriptors (UCUM annotation, issue #2419).
    pub const FDS: &str = "{fd}";
    /// A count of channel entries / queued items (UCUM annotation, issue #2419).
    pub const ENTRIES: &str = "{entry}";
}

/// Bounded attribute keys for catalog metrics.
///
/// These are the ONLY attribute keys downstream code should attach to the
/// catalog metrics. Each is documented with its allowed value space so the
/// cardinality stays bounded.
pub mod attr {
    /// Low-cardinality error category. Values come from
    /// [`crate::observability::ErrorCategory::as_str`] (≈10 distinct values).
    pub const ERROR_CATEGORY: &str = "cqlite.error.category";
    /// Subsystem that produced an event, e.g. `"reader"`, `"query"`,
    /// `"compaction"`. Callers pass a `&'static str`, so the value space is
    /// bounded by the code itself.
    pub const SUBSYSTEM: &str = "cqlite.subsystem";
    /// SSTable on-disk format family, e.g. `"big"` or `"bti"`. Bounded.
    pub const SSTABLE_FORMAT: &str = "cqlite.sstable.format";
    /// Compression algorithm, e.g. `"lz4"`, `"snappy"`, `"none"`. Bounded.
    pub const COMPRESSION: &str = "cqlite.compression";
    /// Outcome of a lookup/check, exactly `"hit"` or `"miss"`. Bounded to two
    /// values; used by partition-lookup and bloom-check counters so a single
    /// metric carries both arms for ratio dashboards.
    pub const RESULT: &str = "cqlite.result";
    /// Read-path access route for a partition lookup, e.g. `"index"` (BIG
    /// Index.db) or `"bti_trie"` (BTI Partitions.db). Bounded by the code.
    /// Distinct from [`ACCESS_PATH`], which is the query-engine SELECT access
    /// path (#1035); this is the storage-layer lookup route (#1034).
    pub const LOOKUP_ROUTE: &str = "cqlite.read.lookup_route";
    /// Access path a `SELECT` chose for its SSTable-scan step (issue #1035).
    ///
    /// Values come from [`crate::query::access_path::AccessPath::label`] — a
    /// closed set such as `"full_scan"`, `"partition_lookup"`,
    /// `"multi_partition_lookup"`, `"clustering_slice"`,
    /// `"fallback_full_scan"`. Bounded by the `AccessPath` enum itself, so it is
    /// safe as a metric dimension and span attribute. NEVER carries key values.
    pub const ACCESS_PATH: &str = "cqlite.query.access_path";
    /// Query plan family chosen by the planner / executor, e.g. `"table_scan"`,
    /// `"point_lookup"`, `"index_scan"`, `"range_scan"`, `"aggregation"`
    /// (issue #1035). Bounded by the executor's plan-type taxonomy.
    pub const PLAN_TYPE: &str = "cqlite.query.plan_type";
    /// Arrow Flight RPC method name (issue #1041), e.g. `"do_get"`,
    /// `"get_flight_info"`, `"get_schema"`, `"handshake"`. Bounded by the
    /// `FlightService` trait's fixed set of methods — never a request payload.
    pub const RPC_METHOD: &str = "cqlite.rpc.method";
    /// Arrow Flight RPC outcome (issue #1041), exactly `"ok"` or `"error"`.
    /// Bounded to two values so a single metric series carries both arms for
    /// success/error-rate dashboards.
    pub const RPC_STATUS: &str = "cqlite.rpc.status";
    /// `do_get` execution phase (issue #2162; `"admission"` added #2420
    /// roborev-1700; `"validate"` added #2420 roborev-1702). Bounded to the
    /// closed set `"validate"`, `"admission"`, `"resolve"`, `"merge_setup"`,
    /// `"stream"` — a `&'static str` from a fixed slot table, never a
    /// per-query, per-ticket, key, or query-text value. Used as the bounded
    /// dimension on [`super::RPC_PHASE_DURATION`] so a stalled `do_get`
    /// localizes to a phase (time piling up in `merge_setup`, queued behind the
    /// admission semaphore in `admission`, or stuck parsing/validating a
    /// malformed ticket in `validate`) from metrics alone.
    pub const RPC_PHASE: &str = "cqlite.rpc.phase";
    /// Reason a `SELECT` fell back to a degraded (full-scan) read path
    /// (issue #2163). Values come from
    /// [`crate::query::access_path::FallbackReason::label`] — a documented closed
    /// set (`no_schema`, `partition_key_not_fully_constrained`,
    /// `partition_key_encoding_failed`, `metadata_scan_path`, `legacy_executor_path`,
    /// `tombstones_build_no_prune`). Bounded by the enum itself; NEVER carries a
    /// partition key, predicate value, or query string.
    pub const FALLBACK_REASON: &str = "cqlite.query.fallback_reason";
    /// Flight warm-handle refresh outcome (issue #2310). Bounded to the closed set
    /// `"unchanged"`, `"rebuilt_delta"`, `"fail_closed_retained"` — a `&'static str`
    /// from a fixed slot table, never a ticket, key, or path value.
    pub const WARM_REFRESH_OUTCOME: &str = "cqlite.warm.refresh_outcome";
}

/// `cqlite.read.rows` — counter `{row}`.
///
/// Total rows materialised by the read path. On the Flight k-way merge scan
/// (issue #2162) the delta is emitted incrementally during a long-running scan,
/// at a bounded row threshold, so the counter climbs before the scan returns;
/// the total is unchanged. That merge-scan emission is FORMAT-AGNOSTIC (carries
/// no attributes): the k-way merge reconciles rows across potentially several
/// input SSTables — of possibly mixed BIG/BTI format — into one row set before
/// this counter's grain, so no single format label is honest at the point of
/// emission without per-input-file tallies threaded through reconciliation (no
/// consumer needs that split today; a future extension could add it). A direct
/// single-SSTable read-path caller may still attach [`attr::SSTABLE_FORMAT`]
/// where the format is known at its own emission site — this metric's attribute
/// set is therefore [`attr::SSTABLE_FORMAT`] OR no attributes, never a fabricated
/// format label.
pub const READ_ROWS: &str = "cqlite.read.rows";

/// `cqlite.read.bytes` — counter `By`.
///
/// Total bytes read from Data.db (post-decompression). Bounded attributes:
/// [`attr::SSTABLE_FORMAT`], [`attr::COMPRESSION`].
pub const READ_BYTES: &str = "cqlite.read.bytes";

/// `cqlite.read.partitions` — counter `{partition}`.
///
/// Total partitions scanned. On the Flight k-way merge scan (issue #2162) the
/// delta is emitted incrementally during a long-running scan, at a bounded row
/// threshold, so the counter climbs before the scan returns; the total is
/// unchanged. Like [`READ_ROWS`], that merge-scan emission is FORMAT-AGNOSTIC
/// (no attributes) — the merged partition already reconciles across possibly
/// mixed-format input SSTables — while a direct single-SSTable read-path caller
/// may still attach [`attr::SSTABLE_FORMAT`]. Bounded attributes:
/// [`attr::SSTABLE_FORMAT`] OR none.
pub const READ_PARTITIONS: &str = "cqlite.read.partitions";

/// `cqlite.read.duration` — histogram `s`.
///
/// Distribution of single read/scan operation durations in seconds. Bounded
/// attributes: [`attr::SSTABLE_FORMAT`].
pub const READ_DURATION: &str = "cqlite.read.duration";

/// `cqlite.sstable.index_parses_total` — counter `1` (issue #2383).
///
/// Incremented ONCE each time the full BIG/NB `Index.db` partition index is
/// parsed end-to-end into partition entries (`parse_all_partition_keys_with_summary`,
/// the O(entries) `memcmp`/vint loop that dominates opening a reader for a
/// many-partition SSTable). It is the scale-free, path-independent probe for the
/// #2383 resolve-phase CPU spin: the field failure re-parsed the SAME 1.58M-entry
/// Index.db 8× for one logical query (redundant per-generation opens across
/// per-query snapshot teardown + un-coalesced concurrent splits), so this counter
/// climbs far past the number of distinct generations. A correct read path parses
/// each generation's Index.db at most ONCE per query (warm reuse / rebind
/// thereafter), so `index_parses_total ≈ Σ generations`, not `Σ generations ×
/// opens`. Emitted from every full-parse site (warm rebuild, aggregate merge,
/// point-read routing) so no single call path can hide a redundant parse. No
/// high-cardinality attributes.
pub const INDEX_PARSES_TOTAL: &str = "cqlite.sstable.index_parses_total";

/// `cqlite.sstable.index_interval_parses_total` — counter `1` (issue #2412).
///
/// Incremented once per **bounded** `Index.db` interval parse performed by the
/// lazy Summary-guided BIG partition index: a point lookup binary-searches
/// `Summary.db`, seeks to the covering sample's position, and parses at most one
/// `min_index_interval` of entries (§B of the #2412 design). This is a DISTINCT
/// counter from [`INDEX_PARSES_TOTAL`], which continues to count only WHOLE-file
/// `Index.db` parses (so a lazy-open regression that accidentally full-parses is
/// still visible there, exactly as the #2367 field rounds check). A cold lazy
/// open of K generations yields `index_parses_total += 0` and
/// `index_interval_parses_total += 0`, then `+= 1` per point lookup — the
/// scale-free work-probe for #2412. No high-cardinality attributes.
pub const INDEX_INTERVAL_PARSES_TOTAL: &str = "cqlite.sstable.index_interval_parses_total";

/// `cqlite.cache.key.hits` — counter `1` (issue #2059).
///
/// Hits on the process-global key→partition-offset cache: a repeated point read
/// whose `(generation identity, raw key)` is resident, so it resolves the partition
/// location WITHOUT reading the Summary-guided `Index.db` interval (post-#2412) or
/// walking the BTI trie. Reported through `Database::stats().memory_stats`.
pub const KEY_CACHE_HITS: &str = "cqlite.cache.key.hits";

/// `cqlite.cache.key.misses` — counter `1` (issue #2059). Misses on the global key
/// cache (including a fail-closed identity mismatch), each paying one interval
/// parse / trie descent then populating.
pub const KEY_CACHE_MISSES: &str = "cqlite.cache.key.misses";

/// `cqlite.cache.key.evictions` — counter `1` (issue #2059). Entries evicted from
/// the global key cache to stay within its byte budget (budget-driven), DISTINCT
/// from [`KEY_CACHE_INVALIDATIONS`].
pub const KEY_CACHE_EVICTIONS: &str = "cqlite.cache.key.evictions";

/// `cqlite.cache.key.invalidations` — counter `1` (issue #2059). Entries dropped on
/// generation removal / compaction / warm-registry evict — DISTINCT from
/// budget-driven [`KEY_CACHE_EVICTIONS`]. A #2383 rebind does NOT invalidate.
pub const KEY_CACHE_INVALIDATIONS: &str = "cqlite.cache.key.invalidations";

/// `cqlite.cache.key.resident_bytes` — gauge `By` (issue #2059). Approximate
/// resident footprint of the global key cache.
pub const KEY_CACHE_RESIDENT_BYTES: &str = "cqlite.cache.key.resident_bytes";

/// `cqlite.cache.key.capacity_bytes` — gauge `By` (issue #2059). The global key
/// cache's fixed byte budget, or `0` when block caching is disabled.
pub const KEY_CACHE_CAPACITY_BYTES: &str = "cqlite.cache.key.capacity_bytes";

/// `cqlite.storage.open.sstables` — counter `{sstable}`.
///
/// SSTables discovered and opened by a single [`StorageEngine`] open, summed
/// across opens over the process lifetime. No high-cardinality attributes.
pub const STORAGE_OPEN_SSTABLES: &str = "cqlite.storage.open.sstables";

/// `cqlite.storage.open.bytes` — counter `By`.
///
/// Total on-disk Data.db bytes across the SSTables discovered by a
/// [`StorageEngine`] open. No high-cardinality attributes.
pub const STORAGE_OPEN_BYTES: &str = "cqlite.storage.open.bytes";

/// `cqlite.storage.open.tables` — counter `1`.
///
/// Total logical tables represented by the SSTables discovered at
/// [`StorageEngine`] open. No high-cardinality attributes.
pub const STORAGE_OPEN_TABLES: &str = "cqlite.storage.open.tables";

/// `cqlite.read.partition_lookup.total` — counter `1`.
///
/// Total partition point lookups attempted on the read path, one increment per
/// lookup. Bounded attributes: [`attr::RESULT`] (`hit`/`miss`),
/// [`attr::LOOKUP_ROUTE`] (`index`/`bti_trie`), and [`attr::SSTABLE_FORMAT`].
/// Carrying `result` as an attribute (instead of separate metric names) lets a
/// dashboard compute hit ratio from one series.
pub const READ_PARTITION_LOOKUP: &str = "cqlite.read.partition_lookup.total";

/// `cqlite.read.bloom.checks` — counter `1`.
///
/// Total bloom-filter / BTI-trie present-or-absent checks on the read path, one
/// increment per check. Bounded attributes: [`attr::RESULT`] (`hit` = maybe
/// present, `miss` = definitely absent) and [`attr::SSTABLE_FORMAT`]. The
/// miss-rate of this metric is the pruning effectiveness; pairing it with
/// [`READ_PARTITION_LOOKUP`] reveals the bloom false-positive rate.
pub const READ_BLOOM_CHECKS: &str = "cqlite.read.bloom.checks";

/// `cqlite.read.scan.window_refill` — counter `1`.
///
/// Incremented once each time the user-facing windowed streaming scan
/// (issue #1143, `run_scan_stream_windowed`) stops at `ParseStep::NeedMore`
/// because the trailing partition straddles a compression-chunk boundary and
/// the driver must await the next decompressed chunk before re-parsing. A
/// non-zero value proves the sliding-window stitch boundary path was actually
/// exercised (a multi-chunk SSTable with a straddling partition); it stays
/// zero for single-chunk SSTables. No high-cardinality attributes.
pub const READ_SCAN_WINDOW_REFILL: &str = "cqlite.read.scan.window_refill";

/// `cqlite.read.sstables_pruned` — counter `{sstable}` (issue #2163).
///
/// Incremented once for each SSTable EXCLUDED from a read because its presence
/// oracle returned a definitive negative — the bloom filter for BIG
/// (`might_contain_partition == false`) or the Partitions.db trie for BTI (a trie
/// miss). Bounded attributes: [`attr::SSTABLE_FORMAT`]. Distinct from
/// [`READ_BLOOM_CHECKS`], which counts *checks* (hit/miss); this counts SSTables
/// actually skipped, in `{sstable}` units — the dashboard-honest prune signal.
pub const READ_SSTABLES_PRUNED: &str = "cqlite.read.sstables_pruned";

/// `cqlite.read.bloom.false_negatives` — counter `1` (issue #2163).
///
/// OPT-IN, default-OFF soundness alarm: incremented ONLY when the presence-oracle
/// false-negative verification (`CQLITE_VERIFY_PRESENCE_ORACLE`) is enabled and an
/// AUTHORITATIVE scan of an SSTable finds a key its bloom/BTI-trie said was
/// definitely absent. Under a correct oracle this stays 0; a non-zero value is a
/// corruption/soundness alarm. Bounded attributes: [`attr::SSTABLE_FORMAT`].
pub const READ_BLOOM_FALSE_NEGATIVES: &str = "cqlite.read.bloom.false_negatives";

/// `cqlite.merge.rows_in` — counter `{row}` (issue #2163).
///
/// Sum of input rows consumed at the k-way merge RECONCILE boundary, emitted once
/// per merge (aggregated from stack-local counters, never per row/cell). Paired
/// with [`MERGE_ROWS_OUT`]; their delta is the number of rows removed by
/// reconciliation (LWW collapse + tombstone suppression). Scoped to reconcile so
/// producer-level filtering (token prune, predicate, `LIMIT`) is EXCLUDED. No
/// high-cardinality attributes.
pub const MERGE_ROWS_IN: &str = "cqlite.merge.rows_in";

/// `cqlite.merge.rows_out` — counter `{row}` (issue #2163).
///
/// Sum of rows emitted by the k-way merge reconcile boundary post-reconciliation,
/// emitted once per merge. See [`MERGE_ROWS_IN`] for the pairing/scope contract.
/// No high-cardinality attributes.
pub const MERGE_ROWS_OUT: &str = "cqlite.merge.rows_out";

/// `cqlite.query.duration` — histogram `s`.
///
/// Distribution of end-to-end query execution durations in seconds. Bounded
/// attributes: [`attr::SUBSYSTEM`]. NEVER attach the query text.
pub const QUERY_DURATION: &str = "cqlite.query.duration";

/// `cqlite.query.rows` — counter `{row}`.
///
/// Total rows returned to callers by the query engine. Bounded attributes:
/// [`attr::ACCESS_PATH`], [`attr::PLAN_TYPE`]. No high-cardinality attributes.
pub const QUERY_ROWS: &str = "cqlite.query.rows";

/// `cqlite.query.rows_scanned` — counter `{row}`.
///
/// Total rows materialised/examined by the SELECT scan step before predicate
/// filtering, projection, and `LIMIT` (issue #1035). The gap between this and
/// [`QUERY_ROWS`] is the read-amplification of a query — large for a
/// `full_scan`, ~1 for a `partition_lookup`. Emitted incrementally during a
/// long-running scan (issue #2162): monotonic counter deltas at a bounded row
/// threshold on the merge/scan loop, plus a final remainder flush, so the counter
/// climbs before the scan returns. The total over a completed scan is identical
/// to the pre-#2162 single end-of-scan emission and it still carries only the
/// bounded [`attr::ACCESS_PATH`] attribute. Bounded attributes:
/// [`attr::ACCESS_PATH`]. Emitted by the modern `SelectExecutor` and the Flight
/// merge/scan loop.
pub const QUERY_ROWS_SCANNED: &str = "cqlite.query.rows_scanned";

/// `cqlite.sstables.open` — gauge `{sstable}`.
///
/// Number of SSTables currently held open. Bounded attributes:
/// [`attr::SSTABLE_FORMAT`].
pub const SSTABLES_OPEN: &str = "cqlite.sstables.open";

/// `cqlite.compaction.duration` — histogram `s`.
///
/// Distribution of compaction run durations in seconds. No high-cardinality
/// attributes.
pub const COMPACTION_DURATION: &str = "cqlite.compaction.duration";

// ---------------------------------------------------------------------------
// Write path (issue #1036) — emitted from the `write-support` write engine.
// ---------------------------------------------------------------------------

/// `cqlite.write.mutations` — counter `{row}`.
///
/// Total mutations accepted by the write path (one per `write`/`write_async`
/// call that successfully inserts into the memtable). No high-cardinality
/// attributes.
pub const WRITE_MUTATIONS: &str = "cqlite.write.mutations";

/// `cqlite.memtable.size_bytes` — gauge `By`.
///
/// Current approximate in-memory size of the active memtable in bytes. No
/// high-cardinality attributes.
pub const MEMTABLE_SIZE_BYTES: &str = "cqlite.memtable.size_bytes";

/// `cqlite.memtable.rows` — gauge `{row}`.
///
/// Current number of buffered rows in the active memtable. No high-cardinality
/// attributes.
pub const MEMTABLE_ROWS: &str = "cqlite.memtable.rows";

/// `cqlite.wal.sync.duration` — histogram `s`.
///
/// Distribution of WAL `fsync` durations in seconds. No high-cardinality
/// attributes.
pub const WAL_SYNC_DURATION: &str = "cqlite.wal.sync.duration";

/// `cqlite.flush.duration` — histogram `s`.
///
/// Distribution of memtable→SSTable flush durations in seconds. No
/// high-cardinality attributes.
pub const FLUSH_DURATION: &str = "cqlite.flush.duration";

/// `cqlite.flush.rows` — counter `{row}`.
///
/// Total rows flushed from the memtable to L0 SSTables. No high-cardinality
/// attributes.
pub const FLUSH_ROWS: &str = "cqlite.flush.rows";

/// `cqlite.flush.bytes` — counter `By`.
///
/// Total Data.db bytes produced by memtable flushes. No high-cardinality
/// attributes.
pub const FLUSH_BYTES: &str = "cqlite.flush.bytes";

/// `cqlite.flush.sstables` — counter `{sstable}`.
///
/// Total L0 SSTables created by memtable flushes. No high-cardinality
/// attributes.
pub const FLUSH_SSTABLES: &str = "cqlite.flush.sstables";

/// `cqlite.write.partitions` — counter `{partition}`.
///
/// Total partitions written by the SSTable writer (flush + compaction output).
/// No high-cardinality attributes.
pub const WRITE_PARTITIONS: &str = "cqlite.write.partitions";

/// `cqlite.write.bytes` — counter `By`.
///
/// Total Data.db bytes produced by the SSTable writer across all output
/// components' Data.db. No high-cardinality attributes.
pub const WRITE_BYTES: &str = "cqlite.write.bytes";

/// `cqlite.compression.ratio` — histogram `1`.
///
/// Per-chunk compression ratio (compressed bytes / uncompressed bytes; ≤1.0
/// means the chunk shrank). Bounded attributes: [`attr::COMPRESSION`].
pub const COMPRESSION_RATIO: &str = "cqlite.compression.ratio";

// ---------------------------------------------------------------------------
// Compaction & maintenance (issue #1037) — emitted from the write engine's
// STCS compaction/k-way-merge path (write-support).
// ---------------------------------------------------------------------------

/// `cqlite.compaction.rows_merged` — counter `{row}`.
///
/// Total rows emitted by the k-way merge across all compactions. Combined with
/// [`COMPACTION_DURATION`] this yields rows-merged-per-second throughput. No
/// high-cardinality attributes.
pub const COMPACTION_ROWS_MERGED: &str = "cqlite.compaction.rows_merged";

/// `cqlite.compaction.bytes_written` — counter `By`.
///
/// Total bytes written to compaction output SSTables (all components). No
/// high-cardinality attributes.
pub const COMPACTION_BYTES_WRITTEN: &str = "cqlite.compaction.bytes_written";

/// `cqlite.compaction.sstables_in` — counter `{sstable}`.
///
/// Total input SSTables consumed by compactions. No high-cardinality
/// attributes.
pub const COMPACTION_SSTABLES_IN: &str = "cqlite.compaction.sstables_in";

/// `cqlite.compaction.sstables_out` — counter `{sstable}`.
///
/// Total output SSTables produced by compactions. No high-cardinality
/// attributes.
pub const COMPACTION_SSTABLES_OUT: &str = "cqlite.compaction.sstables_out";

/// `cqlite.compaction.tombstones_purged` — counter `{tombstone}`.
///
/// Total tombstones GENUINELY PURGED (gc_grace / overlap-safe) during compaction,
/// summed across cell tombstones, whole-row tombstones, range-tombstone markers,
/// and complex-deletion (collection/UDT) markers. Counted only at the actual
/// purge decision points in the merge/reconcile logic; ordinary last-write-wins
/// reconciliation collapse is NOT counted. No high-cardinality attributes.
pub const COMPACTION_TOMBSTONES_PURGED: &str = "cqlite.compaction.tombstones_purged";

/// `cqlite.compaction.tombstones_suppressed` — counter `{tombstone}` (issue #2163).
///
/// Live cells/rows SHADOWED (suppressed) by a tombstone during merge
/// reconciliation, emitted once per merge. Distinct from
/// [`COMPACTION_TOMBSTONES_PURGED`] (a genuine gc/overlap-safe purge) and
/// [`COMPACTION_TOMBSTONES_EMITTED`] (a marker retained). Suppression without a
/// safe purge or a retained marker is the resurrection-risk smell. No
/// high-cardinality attributes.
pub const COMPACTION_TOMBSTONES_SUPPRESSED: &str = "cqlite.compaction.tombstones_suppressed";

/// `cqlite.compaction.tombstones_emitted` — counter `{tombstone}` (issue #2163).
///
/// Tombstone markers RETAINED into the merge output (a row / range / partition
/// / cell tombstone carried forward because it is not purgeable — a cell
/// tombstone counts here too, roborev r7: it is exactly the marker
/// tombstone-resurrection debugging needs to see, not only the coarser
/// row/range/partition markers), emitted once per merge. Distinct from
/// [`COMPACTION_TOMBSTONES_PURGED`] and [`COMPACTION_TOMBSTONES_SUPPRESSED`].
/// No high-cardinality attributes.
pub const COMPACTION_TOMBSTONES_EMITTED: &str = "cqlite.compaction.tombstones_emitted";

/// `cqlite.query.degraded_path.total` — counter `1` (issue #2163).
///
/// Incremented once each time a `SELECT` takes a soundness fallback recorded as
/// [`crate::query::access_path::AccessPath::FallbackFullScan`]. Bounded attribute:
/// [`attr::FALLBACK_REASON`] (the closed `FallbackReason::label()` set). A green
/// targeted query does NOT increment it. NEVER carries a key/predicate/query text.
pub const QUERY_DEGRADED_PATH: &str = "cqlite.query.degraded_path.total";

/// `cqlite.compaction.lag` — gauge `{sstable}`.
///
/// Current L0 SSTables pending compaction (compaction lag). No high-cardinality
/// attributes.
pub const COMPACTION_LAG: &str = "cqlite.compaction.lag";

/// `cqlite.compaction.finalize.duration` — histogram `s`.
///
/// Distribution of compaction finalize (atomic rename / publication-barrier)
/// durations in seconds. No high-cardinality attributes.
pub const COMPACTION_FINALIZE_DURATION: &str = "cqlite.compaction.finalize.duration";

/// `cqlite.compaction.budget.requested` — histogram `s`.
///
/// Distribution of maintenance budget requested per `maintenance_step` call, in
/// seconds. No high-cardinality attributes.
pub const COMPACTION_BUDGET_REQUESTED: &str = "cqlite.compaction.budget.requested";

/// `cqlite.compaction.budget.consumed` — histogram `s`.
///
/// Distribution of maintenance budget actually consumed per `maintenance_step`
/// call, in seconds (compare against [`COMPACTION_BUDGET_REQUESTED`] to track
/// the ~10% tolerance honored by the scheduler). No high-cardinality
/// attributes.
pub const COMPACTION_BUDGET_CONSUMED: &str = "cqlite.compaction.budget.consumed";

/// `cqlite.merge.producer_threads` — gauge `{thread}` (issue #2316).
///
/// Live count of OS producer threads the k-way merge currently has open — one per
/// input SSTable being scanned. The merge (shared by the write-engine
/// compaction/maintenance path and the Flight `do_get` streaming egress) opens one
/// producer thread per input; this gauge makes the previously-invisible per-merge
/// thread cost observable, so the O(M) bound (issue #2316, replacing the old
/// `M·num_cpus` amplification) is assertable on a loaded node. It RISES as a merge
/// spawns its producers (bounded by `O(M)`) and RETURNS to its baseline once those
/// producers are joined/dropped at merge completion. The metric name is coordinated
/// with epic #2313 WS2 (the thread/blocking-pool metrics surface) to avoid a
/// naming collision. No high-cardinality attributes.
pub const MERGE_PRODUCER_THREADS: &str = "cqlite.merge.producer_threads";

/// `cqlite.merge.egress_channel_depth` — gauge `{entry}` (issue #2419, WS2).
///
/// Live occupancy of the bounded producer→consumer `sync_channel` (capacity
/// `STREAMING_CHANNEL_CAPACITY` = 256, `merge/mod.rs`) that carries merged
/// entries from each per-input producer thread toward the consumer (the k-way
/// merge that feeds the Flight `do_get` egress or the write-engine compaction
/// output). `std::sync::mpsc::sync_channel` exposes no `len()`, so occupancy is
/// tracked by a process-wide atomic incremented on a successful data-entry send
/// and decremented on the matching receive (mirroring the #2316
/// `producer_threads` gauge pattern), floored at 0.
///
/// **Healthy vs alarming**: a depth near zero means the consumer is keeping up
/// (or a producer is stalled, e.g. disk-bound — cross-check `cqlite.rpc.rows`);
/// a depth riding near the channel capacity means the producer is outrunning a
/// slower consumer (the egress is back-pressured, distinguishing a "stuck in
/// `do_get`" stall from a disk-bound one). OS-independent (always emits, on
/// every platform), unlike the `cqlite.proc.*` gauges. No high-cardinality
/// attributes. Lives in `cqlite.merge.*` alongside [`MERGE_PRODUCER_THREADS`]
/// (both merge-scoped, shared by compaction + Flight).
pub const MERGE_EGRESS_CHANNEL_DEPTH: &str = "cqlite.merge.egress_channel_depth";

// ---------------------------------------------------------------------------
// Saturation instrumentation (issue #2419, WS2 of epic #2313) — process-wide
// OS-resource gauges + a flight blocking-task proxy, so the read-throughput
// saturation ramp can attribute a plateau to the resource that binds first
// (thread/scheduler collapse → queueing → fd exhaustion → memory). The
// `cqlite.proc.*` gauges are sampled on Linux via `/proc/self/*`; on a
// non-`/proc` platform the reader returns None and the sampler emits NO sample
// (absence, never a fabricated 0 — the telemetry authoritative-data rule #2314).
// ---------------------------------------------------------------------------

/// `cqlite.proc.threads` — gauge `{thread}` (issue #2419, WS2).
///
/// Process-wide OS thread count, sampled from `/proc/self/task` on Linux by the
/// background saturation sampler (~2s cadence). Aggregates the thread footprint
/// across ALL concurrent queries (unlike [`MERGE_PRODUCER_THREADS`], which is
/// per-merge), so N wide `do_get` merges over-subscribing the box is legible on
/// the server's own metric surface, not only through out-of-band `kubectl top`.
///
/// **Healthy vs alarming**: rises with concurrent scans (each opens producer
/// threads) and settles back toward baseline as they complete; a level that
/// keeps climbing toward the container thread ceiling is the thread/scheduler
/// collapse the ramp watches for. **Absence rule**: on a non-`/proc` platform
/// the reader returns None and this gauge is ABSENT from the exposition (never
/// `0`). No high-cardinality attributes.
pub const PROC_THREADS: &str = "cqlite.proc.threads";

/// `cqlite.proc.fds` — gauge `{fd}` (issue #2419, WS2).
///
/// Process-wide open file-descriptor count, sampled from `/proc/self/fd` on
/// Linux. The read path opens a fresh `File` per SSTable per scan (no reader
/// pool, by #815 design), so N×M fds accumulate against a container ulimit
/// (~1024) → `EMFILE`. This gauge makes fd pressure visible before exhaustion.
///
/// **Healthy vs alarming**: rises as concurrent scans open SSTables and falls as
/// they complete; a level approaching the ulimit is the fd-exhaustion binding
/// point. **Absence rule**: None off-Linux → the gauge is absent (never `0`). No
/// high-cardinality attributes.
pub const PROC_FDS: &str = "cqlite.proc.fds";

/// `cqlite.proc.rss_bytes` — gauge `By` (issue #2419, WS2).
///
/// Process resident set size in bytes, sampled from the `VmRSS` field of
/// `/proc/self/status` on Linux (dependency-free plain-text read, no page-size
/// math). The Flight path bypasses the query engine's result-byte budget, so RSS
/// ≈ N × per-scan peak; this gauge makes process memory pressure legible.
///
/// **Healthy vs alarming**: rises with concurrent in-flight scan payloads and
/// falls as they drain; a level approaching the container memory limit is the
/// memory-binding point (and the OOMKill risk). **Absence rule**: None off-Linux
/// → the gauge is absent (never `0`). No high-cardinality attributes.
pub const PROC_RSS_BYTES: &str = "cqlite.proc.rss_bytes";

/// `cqlite.flight.blocking_tasks_in_use` — gauge `{thread}` (issue #2419, WS2).
///
/// Flight-managed `spawn_blocking` tasks currently outstanding, tracked by a
/// process-wide atomic incremented on entry to a flight `spawn_blocking`
/// closure and decremented on exit via an RAII guard (so a panic / cancel /
/// early-return still decrements). Guards EVERY flight-managed blocking closure
/// (roborev job 1733 fix 3, so the gauge reflects true pool-saturation
/// pressure, not merge-only): the streaming merge and aggregate-materialize
/// closures (`streaming.rs`), `do_get`'s resolve-phase closure (producer/schema
/// construction + `DirSource::resolve` + token-prune, `service.rs`), and the
/// `table_stats` `gather_table_stats` closure (`service.rs`). An honest,
/// dependency-free proxy for blocking-pool pressure.
///
/// **Scope caveat**: this is FLIGHT-MANAGED-TASKS-IN-FLIGHT, NOT the global
/// `tokio` blocking-pool queue depth (which needs a build-wide `tokio_unstable`
/// cfg — out of scope, design open fork O1). It never records a fabricated
/// global-pool number. **Healthy vs alarming**: rises with concurrent `do_get`
/// scans and returns to baseline as they finish; a level pinned near the
/// blocking-pool size (~512 default) with flat `cqlite.rpc.rows` is the
/// blocking-pool-saturation smell. OS-independent (always emits). DISTINCT from
/// [`FLIGHT_ADMISSION_IN_USE`] (held admission permits) — the two measure
/// different resources. No high-cardinality attributes.
pub const FLIGHT_BLOCKING_TASKS_IN_USE: &str = "cqlite.flight.blocking_tasks_in_use";

/// `cqlite.errors.total` — counter `{error}`.
///
/// Total errors observed, the canonical error-rate signal (issue #1038).
/// Bounded attributes: [`attr::ERROR_CATEGORY`] and [`attr::SUBSYSTEM`] ONLY.
/// The raw error message is never attached.
///
/// **Eagerly registered at 0 on startup (issue #2288).** When the
/// `observability` feature is active, [`crate::observability::init`] emits a
/// single `add(0)` with an empty attribute set so this counter is present at `0`
/// in a scrape of a freshly-started server, before any error. This makes "metric
/// name absent from the backend" unambiguously mean *error counting isn't wired*
/// (never *no errors occurred yet*), which cost real diagnostic time during the
/// #2193 round-4 field investigation. Real errors add their own labeled series
/// alongside the unlabeled baseline.
///
/// Limitation (per the #2193 code audit): a peer connection RESET that arrives
/// *after* the gRPC `END_STREAM` frame is handled entirely inside the h2/tonic
/// transport and is invisible at this application layer, so it is not counted
/// here. Such post-END_STREAM resets are an expected, benign transport event, not
/// an application error.
pub const ERRORS_TOTAL: &str = "cqlite.errors.total";

// ---------------------------------------------------------------------------
// Arrow Flight gRPC service (issue #1041) — emitted from `cqlite-flight`.
// ---------------------------------------------------------------------------

/// `cqlite.rpc.requests` — counter `1`.
///
/// Total Arrow Flight RPC requests served, one increment per completed RPC.
/// Bounded attributes: [`attr::RPC_METHOD`] (fixed `FlightService` method set)
/// and [`attr::RPC_STATUS`] (`ok`/`error`) so a dashboard computes per-method
/// error rate from one series. NEVER carries request payloads or ticket data.
pub const RPC_REQUESTS: &str = "cqlite.rpc.requests";

/// `cqlite.rpc.duration` — histogram `s`.
///
/// Distribution of Arrow Flight RPC handler durations in seconds (handler entry
/// to response/stream construction). Bounded attributes: [`attr::RPC_METHOD`],
/// [`attr::RPC_STATUS`].
pub const RPC_DURATION: &str = "cqlite.rpc.duration";

/// `cqlite.rpc.in_flight` — gauge `1`.
///
/// Number of Arrow Flight RPCs currently being handled (incremented on entry,
/// decremented on completion). Bounded attributes: [`attr::RPC_METHOD`].
pub const RPC_IN_FLIGHT: &str = "cqlite.rpc.in_flight";

/// `cqlite.rpc.rows` — counter `{row}`.
///
/// Total rows returned to clients by `do_get` (summed across emitted record
/// batches). Emitted incrementally during a long-running scan (issue #2162): a
/// monotonic counter delta per record batch as it passes toward the client, so a
/// climbing value reads as a healthy long scan and a flat one (while
/// [`RPC_IN_FLIGHT`] > 0) as a stall. The counter total over a fully-drained
/// stream is unchanged — only the emission cadence moved from stream-end to
/// per-batch. Bounded attributes: [`attr::RPC_METHOD`].
pub const RPC_ROWS: &str = "cqlite.rpc.rows";

/// `cqlite.rpc.bytes` — counter `By`.
///
/// Total record-batch payload bytes streamed to clients by `do_get` (in-memory
/// Arrow batch size, pre-IPC-framing). Emitted incrementally during a
/// long-running scan (issue #2162): a monotonic counter delta per record batch;
/// the total over a fully-drained stream is byte-identical to the pre-#2162
/// single end-of-stream emission. Bounded attributes: [`attr::RPC_METHOD`].
pub const RPC_BYTES: &str = "cqlite.rpc.bytes";

/// `cqlite.rpc.phase.duration` — histogram `s` (issue #2162; `admission` phase
/// added #2420 roborev-1700; `validate` phase added #2420 roborev-1702).
///
/// Wall time a `do_get` spends in each of a bounded, closed set of execution
/// phases — `validate` (parsing the ticket bytes, BEFORE any admission-permit
/// acquire — a syntactically malformed ticket records ONLY this phase),
/// `admission` (queued waiting for, or immediately granted, an admission permit
/// — see #2420 — AFTER validation but BEFORE any producer/schema construction
/// or filesystem access), `resolve` (producer/schema construction + path
/// discovery/token prune), `merge_setup` (opening input SSTables + building the
/// k-way merger, the #2157 stall suspect), and `stream` (partitions stepping +
/// batches flowing to the client). Recorded once per phase transition, so a
/// `do_get` dominated by opening SSTables shows its wall time accumulating in
/// `merge_setup` BEFORE the first batch, and a `do_get` queued behind a
/// saturated admission ceiling shows it accumulating in `admission` BEFORE
/// `resolve` even starts — a stall (or queueing delay, or a flood of malformed
/// tickets) that emits zero rows still localizes to a phase. `cqlite.rpc.duration`
/// already includes admission wait time in the RPC total; this is the per-phase
/// breakdown field triage uses to localize WHERE that time went (e.g. #2398).
/// Bounded attributes: [`attr::RPC_METHOD`], [`attr::RPC_PHASE`] (the closed
/// five-value set). NEVER carries a ticket, key, token range, or query-text
/// attribute.
pub const RPC_PHASE_DURATION: &str = "cqlite.rpc.phase.duration";

/// `cqlite.rpc.phase.active` — gauge `1` (issue #2361; `admission` phase added
/// #2420 roborev-1700; `validate` phase added #2420 roborev-1702).
///
/// In-flight visibility of the phase a `do_get` is CURRENTLY executing, set to 1
/// on phase entry and back to 0 on exit (via [`super::super`]'s `PhaseTimer`
/// transition/`Drop`). [`RPC_PHASE_DURATION`] only records a sample once a phase
/// COMPLETES, so a `do_get` wedged forever in `stream` (the #2361 hang: a merge
/// that never returns a batch) recorded NOTHING — this gauge shows `stream = 1`
/// for the entire hang, so a stall is observable BEFORE completion; likewise a
/// `do_get` queued behind a saturated admission ceiling shows `admission = 1`
/// for the whole wait. Bounded attributes: [`attr::RPC_METHOD`],
/// [`attr::RPC_PHASE`] (the closed five-value set) — low cardinality (methods ×
/// 5 phases). NEVER a ticket/key/query value.
pub const RPC_PHASE_ACTIVE: &str = "cqlite.rpc.phase.active";

/// `cqlite.warm.cache.hits` — counter `{1}` (issue #2310).
///
/// Flight warm-handle cache hits: a request whose probed SSTable generation set
/// matched the cached set, served from warm parsed state with ZERO reader-open
/// and zero Index/Summary/Statistics/bloom parse. No attributes (bounded).
pub const WARM_CACHE_HITS: &str = "cqlite.warm.cache.hits";

/// `cqlite.warm.cache.misses` — counter `{1}` (issue #2310).
///
/// Flight warm-handle cache misses: no cached entry, or the generation set
/// changed so a (delta) rebuild was required. No attributes (bounded).
pub const WARM_CACHE_MISSES: &str = "cqlite.warm.cache.misses";

/// `cqlite.warm.cache.evicts` — counter `{1}` (issue #2310).
///
/// Warm generations evicted, whether by LRU (byte-budget pressure) or because a
/// rebuild found them removed on disk. No attributes (bounded).
pub const WARM_CACHE_EVICTS: &str = "cqlite.warm.cache.evicts";

/// `cqlite.warm.cache.refresh` — counter `{1}` (issue #2310).
///
/// Warm-handle refresh outcomes, tagged by [`attr::WARM_REFRESH_OUTCOME`]
/// (`unchanged` / `rebuilt_delta` / `fail_closed_retained`) — the single bounded
/// dimension. Distinguishes a warm hit from a delta rebuild from a fail-closed
/// retention in metrics alone (spec Requirement 6).
pub const WARM_CACHE_REFRESH: &str = "cqlite.warm.cache.refresh";

/// `cqlite.flight.admission.limit` — gauge `1` (issue #2420, WS4).
///
/// The configured `do_get` admission ceiling `K` (the `--max-concurrent-scans`
/// value). A constant level while the server runs; recorded on startup so a
/// dashboard can chart `in_use` against the limit. No attributes (bounded).
/// DISTINCT from [`RPC_IN_FLIGHT`]: this is the CONFIGURED ceiling, not a live
/// count.
pub const FLIGHT_ADMISSION_LIMIT: &str = "cqlite.flight.admission.limit";

/// `cqlite.flight.admission.in_use` — gauge `1` (issue #2420, WS4).
///
/// `do_get` admission permits currently held — the number of scans ADMITTED and
/// in-flight (an up/down level like [`RPC_IN_FLIGHT`], but counting only admitted
/// scans, not every accepted RPC incl. the ones parked waiting for a permit).
/// Returns to zero when every admitted scan completes/cancels/disconnects (the
/// RAII permit release). No attributes (bounded). DISTINCT from [`RPC_IN_FLIGHT`].
pub const FLIGHT_ADMISSION_IN_USE: &str = "cqlite.flight.admission.in_use";

/// `cqlite.flight.admission.waiting` — gauge `1` (issue #2420, WS4).
///
/// `do_get` requests currently parked on `acquire`, waiting for an admission
/// permit to free within the permit-wait timeout. A non-zero value is the
/// backpressure signal: offered concurrency has exceeded the ceiling and requests
/// are queuing rather than degrading together. No attributes (bounded).
pub const FLIGHT_ADMISSION_WAITING: &str = "cqlite.flight.admission.waiting";

/// `cqlite.flight.admission.rejected_total` — counter `{1}` (issue #2420, WS4).
///
/// `do_get` requests rejected because no admission permit freed within the
/// permit-wait timeout — each returned to the client as gRPC `UNAVAILABLE` (so the
/// connector's #2241 replica-failover treats it as retry-safe), before any record
/// batch was delivered. A monotonic total; scale-free. No attributes (bounded).
pub const FLIGHT_ADMISSION_REJECTED_TOTAL: &str = "cqlite.flight.admission.rejected_total";

/// `cqlite.flight.admission.wait_seconds` — histogram `s` (issue #2420, WS4).
///
/// Distribution of how long a `do_get` waited on `acquire` before it was admitted
/// (a permit freed) OR rejected (the wait timeout elapsed). Localizes admission
/// pressure: a rising tail means requests are increasingly queuing for permits.
/// No attributes (bounded).
pub const FLIGHT_ADMISSION_WAIT_SECONDS: &str = "cqlite.flight.admission.wait_seconds";

/// All catalog metric names, for tests and registration sanity checks.
pub const ALL_METRICS: &[&str] = &[
    READ_ROWS,
    READ_BYTES,
    READ_PARTITIONS,
    READ_DURATION,
    READ_PARTITION_LOOKUP,
    READ_BLOOM_CHECKS,
    READ_SCAN_WINDOW_REFILL,
    READ_SSTABLES_PRUNED,
    READ_BLOOM_FALSE_NEGATIVES,
    MERGE_ROWS_IN,
    MERGE_ROWS_OUT,
    QUERY_DEGRADED_PATH,
    INDEX_PARSES_TOTAL,
    INDEX_INTERVAL_PARSES_TOTAL,
    // Global key→partition-offset cache (#2059)
    KEY_CACHE_HITS,
    KEY_CACHE_MISSES,
    KEY_CACHE_EVICTIONS,
    KEY_CACHE_INVALIDATIONS,
    KEY_CACHE_RESIDENT_BYTES,
    KEY_CACHE_CAPACITY_BYTES,
    STORAGE_OPEN_SSTABLES,
    STORAGE_OPEN_BYTES,
    STORAGE_OPEN_TABLES,
    QUERY_DURATION,
    QUERY_ROWS,
    QUERY_ROWS_SCANNED,
    SSTABLES_OPEN,
    COMPACTION_DURATION,
    ERRORS_TOTAL,
    // Write path (#1036)
    WRITE_MUTATIONS,
    MEMTABLE_SIZE_BYTES,
    MEMTABLE_ROWS,
    WAL_SYNC_DURATION,
    FLUSH_DURATION,
    FLUSH_ROWS,
    FLUSH_BYTES,
    FLUSH_SSTABLES,
    WRITE_PARTITIONS,
    WRITE_BYTES,
    COMPRESSION_RATIO,
    // Compaction & maintenance (#1037)
    COMPACTION_ROWS_MERGED,
    COMPACTION_BYTES_WRITTEN,
    COMPACTION_SSTABLES_IN,
    COMPACTION_SSTABLES_OUT,
    COMPACTION_TOMBSTONES_PURGED,
    COMPACTION_TOMBSTONES_SUPPRESSED,
    COMPACTION_TOMBSTONES_EMITTED,
    COMPACTION_LAG,
    COMPACTION_FINALIZE_DURATION,
    COMPACTION_BUDGET_REQUESTED,
    COMPACTION_BUDGET_CONSUMED,
    MERGE_PRODUCER_THREADS,
    // Arrow Flight gRPC service (#1041)
    RPC_REQUESTS,
    RPC_DURATION,
    RPC_IN_FLIGHT,
    RPC_ROWS,
    RPC_BYTES,
    // In-progress read/query metrics (#2162)
    RPC_PHASE_DURATION,
    // In-flight phase gauge (#2361)
    RPC_PHASE_ACTIVE,
    // Flight warm-handle cache (#2310)
    WARM_CACHE_HITS,
    WARM_CACHE_MISSES,
    WARM_CACHE_EVICTS,
    WARM_CACHE_REFRESH,
    // Flight do_get admission control (#2420, WS4)
    FLIGHT_ADMISSION_LIMIT,
    FLIGHT_ADMISSION_IN_USE,
    FLIGHT_ADMISSION_WAITING,
    FLIGHT_ADMISSION_REJECTED_TOTAL,
    FLIGHT_ADMISSION_WAIT_SECONDS,
    // Saturation instrumentation (#2419, WS2 of epic #2313)
    MERGE_EGRESS_CHANNEL_DEPTH,
    PROC_THREADS,
    PROC_FDS,
    PROC_RSS_BYTES,
    FLIGHT_BLOCKING_TASKS_IN_USE,
];

/// The five saturation gauges added by issue #2419 (WS2). Grouped for the
/// distinctness/registration tests and #2426's operator reference so they can be
/// presented as one section without re-listing them by hand.
pub const SATURATION_GAUGES: &[&str] = &[
    MERGE_EGRESS_CHANNEL_DEPTH,
    PROC_THREADS,
    PROC_FDS,
    PROC_RSS_BYTES,
    FLIGHT_BLOCKING_TASKS_IN_USE,
];

/// The five `cqlite.flight.admission.*` gauges/counters from issue #2420 (WS4),
/// grouped so the saturation-family distinctness test can assert the two
/// families are pairwise disjoint (spec Requirement: distinct families).
pub const ADMISSION_METRICS: &[&str] = &[
    FLIGHT_ADMISSION_LIMIT,
    FLIGHT_ADMISSION_IN_USE,
    FLIGHT_ADMISSION_WAITING,
    FLIGHT_ADMISSION_REJECTED_TOTAL,
    FLIGHT_ADMISSION_WAIT_SECONDS,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_names_are_namespaced_and_unique() {
        let mut seen = std::collections::HashSet::new();
        for name in ALL_METRICS {
            assert!(
                name.starts_with("cqlite."),
                "metric {name} must be rooted under cqlite."
            );
            assert!(seen.insert(*name), "duplicate metric name {name}");
        }
        assert_eq!(seen.len(), ALL_METRICS.len());
    }

    #[test]
    fn attribute_keys_are_namespaced() {
        for key in [
            attr::ERROR_CATEGORY,
            attr::SUBSYSTEM,
            attr::SSTABLE_FORMAT,
            attr::COMPRESSION,
            attr::RESULT,
            attr::LOOKUP_ROUTE,
            attr::ACCESS_PATH,
            attr::PLAN_TYPE,
            attr::RPC_METHOD,
            attr::RPC_STATUS,
            attr::RPC_PHASE,
            attr::FALLBACK_REASON,
            attr::WARM_REFRESH_OUTCOME,
        ] {
            assert!(key.starts_with("cqlite."), "attr {key} must be namespaced");
        }
    }

    #[test]
    fn warm_cache_metrics_are_registered_and_namespaced() {
        // Issue #2310: the warm-handle counters must be part of the canonical
        // catalog so registration/uniqueness checks cover them, and rooted under
        // `cqlite.` like every other metric.
        for m in [
            WARM_CACHE_HITS,
            WARM_CACHE_MISSES,
            WARM_CACHE_EVICTS,
            WARM_CACHE_REFRESH,
        ] {
            assert!(ALL_METRICS.contains(&m), "{m} must be catalogued");
            assert!(m.starts_with("cqlite.warm."));
        }
        assert!(attr::WARM_REFRESH_OUTCOME.starts_with("cqlite."));
    }

    #[test]
    fn rpc_phase_duration_is_registered_and_namespaced() {
        // Issue #2162: the new phase-duration histogram must be part of the
        // canonical catalog so registration/uniqueness sanity checks cover it, and
        // its name must be rooted under `cqlite.` like every other metric.
        assert!(ALL_METRICS.contains(&RPC_PHASE_DURATION));
        assert!(RPC_PHASE_DURATION.starts_with("cqlite."));
        assert!(attr::RPC_PHASE.starts_with("cqlite."));
    }

    #[test]
    fn rpc_phase_active_gauge_is_registered_and_namespaced() {
        // Issue #2361: the in-flight phase gauge must be catalogued (so the
        // registration/uniqueness checks cover it) and namespaced like the rest.
        assert!(ALL_METRICS.contains(&RPC_PHASE_ACTIVE));
        assert_eq!(RPC_PHASE_ACTIVE, "cqlite.rpc.phase.active");
        assert!(RPC_PHASE_ACTIVE.starts_with("cqlite."));
    }

    #[test]
    fn index_parses_total_counter_is_registered_and_namespaced() {
        // Issue #2383: the redundant-Index.db-parse probe must be catalogued (so
        // the registration/uniqueness checks cover it) and namespaced.
        assert!(ALL_METRICS.contains(&INDEX_PARSES_TOTAL));
        assert_eq!(INDEX_PARSES_TOTAL, "cqlite.sstable.index_parses_total");
        assert!(INDEX_PARSES_TOTAL.starts_with("cqlite."));
    }

    #[test]
    fn index_interval_parses_counter_is_distinct_registered_and_namespaced() {
        // Issue #2412 spec Requirement 5: the bounded interval-parse counter is a
        // DISTINCT catalog metric (never conflated with full parses), catalogued so
        // the registration/uniqueness checks cover it, and rooted under `cqlite.`.
        assert!(ALL_METRICS.contains(&INDEX_INTERVAL_PARSES_TOTAL));
        assert_eq!(
            INDEX_INTERVAL_PARSES_TOTAL,
            "cqlite.sstable.index_interval_parses_total"
        );
        assert!(INDEX_INTERVAL_PARSES_TOTAL.starts_with("cqlite."));
        // Distinct from the full-parse counter — the two must never collapse to one
        // name (a lazy-open regression must stay visible on INDEX_PARSES_TOTAL).
        assert_ne!(INDEX_INTERVAL_PARSES_TOTAL, INDEX_PARSES_TOTAL);
    }

    #[test]
    fn global_key_cache_counters_are_registered_and_namespaced() {
        // Issue #2059 spec Requirement "Real, cqlite-namespaced observability
        // counters": every key-cache counter/gauge name is in the catalog and rooted
        // under `cqlite.`, with evictions and invalidations kept DISTINCT.
        for name in [
            KEY_CACHE_HITS,
            KEY_CACHE_MISSES,
            KEY_CACHE_EVICTIONS,
            KEY_CACHE_INVALIDATIONS,
            KEY_CACHE_RESIDENT_BYTES,
            KEY_CACHE_CAPACITY_BYTES,
        ] {
            assert!(ALL_METRICS.contains(&name), "{name} must be catalogued");
            assert!(name.starts_with("cqlite."), "{name} must be namespaced");
        }
        assert_ne!(
            KEY_CACHE_EVICTIONS, KEY_CACHE_INVALIDATIONS,
            "budget evictions and generation invalidations are distinct counters"
        );
    }

    #[test]
    fn read_scan_window_refill_counter_is_registered_and_namespaced() {
        // Issue #2426 (roborev MEDIUM): the windowed-scan refill counter is an
        // EMITTED instrument (a dedicated `Instruments` field + emission site in
        // `scan_stream_windowed.rs`), so it MUST be in the canonical catalog or the
        // operator "every instrument" reference silently omits it and the freshness
        // gate cannot see it.
        assert!(ALL_METRICS.contains(&READ_SCAN_WINDOW_REFILL));
        assert_eq!(READ_SCAN_WINDOW_REFILL, "cqlite.read.scan.window_refill");
        assert!(READ_SCAN_WINDOW_REFILL.starts_with("cqlite."));
    }

    #[test]
    fn every_instrument_registered_in_otel_is_catalogued() {
        // Issue #2426 (roborev MEDIUM, F1): guard the "emitted instrument absent
        // from ALL_METRICS" bug class. `otel.rs` is the canonical instrument
        // construction + record-routing site (every cross-crate emission — incl.
        // cqlite-flight's warm-cache/admission metrics — routes through its
        // `add_counter`/`record_histogram`/`record_gauge` dedicated arms). Any
        // `catalog::SCREAMING_CONST` referenced there is a metric name bound to a
        // real instrument, so it MUST appear in `ALL_METRICS`. This is a
        // fully-automatic source-level check (no `observability` feature needed):
        // add an instrument in `otel.rs` and forget to catalogue it → this fails.
        //
        // Automation note (#2426): this scans the core `otel.rs` registration site.
        // Because every catalogued instrument that cqlite-flight emits now has a
        // dedicated arm here (never the ad-hoc `_ =>` fallback), the check
        // transitively covers the flight emission sites too. A future metric emitted
        // ONLY via the ad-hoc fallback (no dedicated arm, no catalog entry) would not
        // be caught here — that path is reserved for genuinely non-catalog names.
        let otel_src = include_str!("otel.rs");
        let catalogued: std::collections::HashSet<&str> = ALL_METRICS.iter().copied().collect();

        // Collect the const IDENTIFIERS present in the ALL_METRICS array so we can
        // map an `otel.rs` `catalog::IDENT` reference to a catalogued name. The
        // constants are `pub const IDENT: &str = "cqlite. …";`, so build the map
        // from this source file.
        let this_src = include_str!("catalog.rs");
        let mut ident_to_value = std::collections::HashMap::new();
        for line in this_src.lines() {
            let line = line.trim_start();
            if let Some(rest) = line.strip_prefix("pub const ") {
                if let Some((ident, tail)) = rest.split_once(':') {
                    if let Some(start) = tail.find('"') {
                        let after = &tail[start + 1..];
                        if let Some(end) = after.find('"') {
                            ident_to_value.insert(ident.trim(), &after[..end]);
                        }
                    }
                }
            }
        }

        // Extract every `catalog::SCREAMING_CONST` reference in otel.rs. `unit`/
        // `attr` submodule refs (`catalog::unit::…`, `catalog::attr::…`) start with
        // a lowercase char after `catalog::`, so they are excluded by construction.
        let mut missing = Vec::new();
        for (i, _) in otel_src.match_indices("catalog::") {
            let rest = &otel_src[i + "catalog::".len()..];
            let ident: String = rest
                .chars()
                .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
                .collect();
            // Skip lowercase submodule paths (unit/attr) — `ident` is empty then.
            if ident.is_empty() {
                continue;
            }
            let value = ident_to_value.get(ident.as_str()).copied().unwrap_or_else(|| {
                panic!("otel.rs references catalog::{ident}, which is not a metric-name constant in catalog.rs")
            });
            if !catalogued.contains(value) {
                missing.push(format!("catalog::{ident} (\"{value}\")"));
            }
        }
        assert!(
            missing.is_empty(),
            "otel.rs registers instruments for metrics ABSENT from ALL_METRICS \
             (add them to catalog::ALL_METRICS): {missing:?}"
        );
    }

    #[test]
    fn saturation_gauges_are_registered_namespaced_and_unique() {
        // Issue #2419 (WS2), spec Requirement: every saturation gauge must be a
        // `cqlite.*` name in ALL_METRICS, appearing exactly once, with the units
        // the design's naming table pins. Fails on `main` until the constants land.
        for m in SATURATION_GAUGES {
            assert!(ALL_METRICS.contains(m), "{m} must be catalogued");
            assert!(m.starts_with("cqlite."), "{m} must be rooted under cqlite.");
            assert_eq!(
                ALL_METRICS.iter().filter(|n| *n == m).count(),
                1,
                "{m} must appear exactly once in ALL_METRICS"
            );
        }
        assert_eq!(
            MERGE_EGRESS_CHANNEL_DEPTH,
            "cqlite.merge.egress_channel_depth"
        );
        assert_eq!(PROC_THREADS, "cqlite.proc.threads");
        assert_eq!(PROC_FDS, "cqlite.proc.fds");
        assert_eq!(PROC_RSS_BYTES, "cqlite.proc.rss_bytes");
        assert_eq!(
            FLIGHT_BLOCKING_TASKS_IN_USE,
            "cqlite.flight.blocking_tasks_in_use"
        );
        // Units from the design naming table (#2419 design D4).
        assert_eq!(unit::FDS, "{fd}");
        assert_eq!(unit::ENTRIES, "{entry}");
        assert_eq!(unit::THREADS, "{thread}");
        assert_eq!(unit::BYTES, "By");
    }

    #[test]
    fn saturation_gauges_have_dedicated_otel_arms_not_the_adhoc_fallback() {
        // Issue #2419 (WS2), spec Requirement / #2412 lesson: each saturation
        // gauge must resolve in `otel::record_gauge` to a pre-built `Instruments`
        // field, NOT the ad-hoc `_ =>` fallback (which rebuilds the instrument on
        // every sample). Source-scan otel.rs for a dedicated `catalog::IDENT =>`
        // match arm per gauge — a fully-automatic check needing no `observability`
        // feature. Delete an arm → this fails.
        let otel_src = include_str!("otel.rs");
        for ident in [
            "MERGE_EGRESS_CHANNEL_DEPTH",
            "PROC_THREADS",
            "PROC_FDS",
            "PROC_RSS_BYTES",
            "FLIGHT_BLOCKING_TASKS_IN_USE",
        ] {
            let arm = format!("catalog::{ident} =>");
            assert!(
                otel_src.contains(&arm),
                "otel::record_gauge lacks a dedicated arm `{arm}` — the gauge would \
                 fall through to the ad-hoc per-call-rebuilt fallback (#2412)"
            );
        }
    }

    #[test]
    fn saturation_family_is_disjoint_from_admission_family() {
        // Issue #2419 (WS2), spec Requirement: the saturation gauges SHALL NOT
        // duplicate or overlap the #2420 admission gauges, and
        // `cqlite.flight.blocking_tasks_in_use` (blocking-pool pressure) must be a
        // DISTINCT metric from `cqlite.flight.admission.in_use` (held permits).
        for s in SATURATION_GAUGES {
            for a in ADMISSION_METRICS {
                assert_ne!(s, a, "saturation gauge {s} collides with admission {a}");
            }
        }
        assert_ne!(FLIGHT_BLOCKING_TASKS_IN_USE, FLIGHT_ADMISSION_IN_USE);
    }

    #[test]
    fn merge_producer_threads_gauge_is_registered_and_documented() {
        // Issue #2316: the merge producer-thread gauge must be part of the
        // canonical catalog (so the registration/uniqueness checks cover it), be
        // rooted under `cqlite.`, and carry the `{thread}` unit agreed with #2313 WS2.
        assert!(ALL_METRICS.contains(&MERGE_PRODUCER_THREADS));
        assert_eq!(MERGE_PRODUCER_THREADS, "cqlite.merge.producer_threads");
        assert!(MERGE_PRODUCER_THREADS.starts_with("cqlite."));
        assert_eq!(unit::THREADS, "{thread}");
    }
}
