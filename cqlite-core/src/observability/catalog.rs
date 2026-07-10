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
    /// `do_get` execution phase (issue #2162). Bounded to the closed set
    /// `"resolve"`, `"merge_setup"`, `"stream"` — a `&'static str` from a fixed
    /// slot table, never a per-query, per-ticket, key, or query-text value. Used
    /// as the bounded dimension on [`super::RPC_PHASE_DURATION`] so a stalled
    /// `do_get` localizes to a phase (time piling up in `merge_setup`) from
    /// metrics alone.
    pub const RPC_PHASE: &str = "cqlite.rpc.phase";
    /// Reason a `SELECT` fell back to a degraded (full-scan) read path
    /// (issue #2163). Values come from
    /// [`crate::query::access_path::FallbackReason::label`] — a documented closed
    /// set (`no_schema`, `partition_key_not_fully_constrained`,
    /// `partition_key_encoding_failed`, `metadata_scan_path`, `legacy_executor_path`,
    /// `tombstones_build_no_prune`). Bounded by the enum itself; NEVER carries a
    /// partition key, predicate value, or query string.
    pub const FALLBACK_REASON: &str = "cqlite.query.fallback_reason";
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
/// [`attr::ACCESS_PATH`] (`index`/`bti_trie`), and [`attr::SSTABLE_FORMAT`].
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

/// `cqlite.errors.total` — counter `{error}`.
///
/// Total errors observed, the canonical error-rate signal (issue #1038).
/// Bounded attributes: [`attr::ERROR_CATEGORY`] and [`attr::SUBSYSTEM`] ONLY.
/// The raw error message is never attached.
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

/// `cqlite.rpc.phase.duration` — histogram `s` (issue #2162).
///
/// Wall time a `do_get` spends in each of a bounded, closed set of execution
/// phases — `resolve` (path discovery + token prune), `merge_setup` (opening
/// input SSTables + building the k-way merger, the #2157 stall suspect), and
/// `stream` (partitions stepping + batches flowing to the client). Recorded once
/// per phase transition, so a `do_get` dominated by opening SSTables shows its
/// wall time accumulating in `merge_setup` BEFORE the first batch — a stall that
/// emits zero rows still localizes to a phase. Bounded attributes:
/// [`attr::RPC_METHOD`], [`attr::RPC_PHASE`] (the closed three-value set). NEVER
/// carries a ticket, key, token range, or query-text attribute.
pub const RPC_PHASE_DURATION: &str = "cqlite.rpc.phase.duration";

/// All catalog metric names, for tests and registration sanity checks.
pub const ALL_METRICS: &[&str] = &[
    READ_ROWS,
    READ_BYTES,
    READ_PARTITIONS,
    READ_DURATION,
    READ_PARTITION_LOOKUP,
    READ_BLOOM_CHECKS,
    READ_SSTABLES_PRUNED,
    READ_BLOOM_FALSE_NEGATIVES,
    MERGE_ROWS_IN,
    MERGE_ROWS_OUT,
    QUERY_DEGRADED_PATH,
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
        ] {
            assert!(key.starts_with("cqlite."), "attr {key} must be namespaced");
        }
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
