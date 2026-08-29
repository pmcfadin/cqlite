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
    /// A count of concurrent k-way merge operations (UCUM annotation, #2765).
    pub const MERGES: &str = "{merge}";
}

/// Bounded attribute keys for catalog metrics.
///
/// These are the ONLY attribute keys downstream code should attach to the
/// catalog metrics. Each is documented with its allowed value space so the
/// cardinality stays bounded.
pub mod attr {
    /// Low-cardinality error category. Values come from
    /// [`crate::observability::ObsErrorCategory::as_str`] (≈10 distinct values).
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
    /// closed set of five TOP-LEVEL values `"validate"`, `"admission"`,
    /// `"resolve"`, `"merge_setup"`, `"stream"` — a `&'static str` from a fixed
    /// slot table, never a per-query, per-ticket, key, or query-text value. Used
    /// as the bounded dimension on [`super::RPC_PHASE_DURATION`] so a stalled
    /// `do_get` localizes to a phase (time piling up in `merge_setup`, queued
    /// behind the admission semaphore in `admission`, or stuck parsing/validating
    /// a malformed ticket in `validate`) from metrics alone.
    ///
    /// Cardinality (#2819; extended by #3096): on [`super::RPC_PHASE_DURATION`] the
    /// `do_get` method ALSO carries SIX in-`stream` sub-phase values
    /// (`"stream_cold_fault"`, `"stream_decompress"`, `"stream_merge"`, `"stream_encode"`,
    /// `"stream_encode_framing"`, `"stream_grpc_write"`), so its phase.duration value
    /// set is ELEVEN. Those values are gated to `do_get` + `phase.duration` only —
    /// they are NOT added to [`super::RPC_PHASE_ACTIVE`], which stays the five
    /// top-level values per method. Still a closed, static, low-cardinality set.
    pub const RPC_PHASE: &str = "cqlite.rpc.phase";
    /// Reason a `SELECT` fell back to a degraded (full-scan) read path
    /// (issue #2163). Values come from
    /// [`crate::query::access_path::FallbackReason::label`] — a documented closed
    /// set (`no_schema`, `partition_key_not_fully_constrained`,
    /// `partition_key_encoding_failed`, `metadata_scan_path`, `legacy_executor_path`,
    /// `tombstones_build_no_prune`). Bounded by the enum itself; NEVER carries a
    /// partition key, predicate value, or query string.
    pub const FALLBACK_REASON: &str = "cqlite.query.fallback_reason";
    /// Structural invariant a BTI `Rows.db` row-index ROOT violated, making the
    /// clustering read fall back to a full-partition decode (issue #3002). Values
    /// come from
    /// [`crate::storage::sstable::bti::RowsTrieRootRejectReason::label`] — a closed,
    /// static set of SEVEN `&'static str`s (`not_below_entry`,
    /// `payload_incapable_node_type`, `childless_root_without_payload`,
    /// `truncated_node`, `sparse_node_without_transitions`, `invalid_payload_bits`,
    /// `extent_not_at_entry`), STAMPED per enum variant and never derived from file
    /// bytes or a message string. Bounded by the enum itself; NEVER carries an
    /// offset, key, or path.
    pub const ROWS_ROOT_REJECT_REASON: &str = "cqlite.read.rows_root_reject_reason";
    /// Flight warm-handle refresh outcome (issue #2310). Bounded to the closed set
    /// `"unchanged"`, `"rebuilt_delta"`, `"fail_closed_retained"` — a `&'static str`
    /// from a fixed slot table, never a ticket, key, or path value.
    pub const WARM_REFRESH_OUTCOME: &str = "cqlite.warm.refresh_outcome";
    /// Fine-grained Flight `do_get` abort reason (issue #2681). Bounded to the
    /// closed set `"superseded_split"`, `"client_cancel"`, `"admission_shed"`,
    /// `"snapshot_retired"`, `"internal"`, `"ticket_invalid"` — a `&'static str`
    /// STAMPED at the abort construction site (never inferred from the gRPC code
    /// or the error message text, no-heuristics #28). Attached ONLY to
    /// [`super::ERRORS_TOTAL`] for `cqlite.subsystem = "flight"`, so a benign
    /// abort (a torn-down split, a client hang-up, an admission shed) is
    /// distinguishable in-field from a genuine internal fault that all previously
    /// collapsed into the coarse `cqlite.error.category = "other"` bucket. The
    /// high-cardinality ticket/split identity + snapshot generation live on the
    /// abort log/trace event only, NEVER on this or any metric label.
    pub const FLIGHT_ABORT_REASON: &str = "cqlite.flight.abort_reason";
    /// Repeat-access bucket for the bounded partition access-distribution probe
    /// (issue #2827). Bounded to the closed set of EXACTLY six labels
    /// `"1"`, `"2"`, `"3-4"`, `"5-8"`, `"9-16"`, `"17+"` — the number of times a
    /// distinct partition was accessed inside one measurement window, summarised
    /// IN-PROCESS into these six buckets before emission.
    ///
    /// The bucket label is the ONLY thing that leaves the process about a
    /// partition's access count: no partition key, key hash, key prefix, key
    /// length or token is ever attached (the binding constraint at the top of
    /// this module). Six values × the three [`SIZE_SOURCE`] values is the whole
    /// cardinality budget of
    /// [`super::READ_PARTITION_ACCESS_DISTINCT_PARTITIONS`] — eighteen series, and
    /// 34 across the whole seven-metric `cqlite.read.partition_access.*` family
    /// (18 + 6 + 6 + four unlabelled scalars).
    pub const REPEAT_BUCKET: &str = "cqlite.read.repeat_bucket";
    /// Provenance of the on-disk byte weight recorded for a partition access
    /// (issue #2827). Bounded to the closed set of EXACTLY three labels:
    ///
    /// - `"successor_gap"` — the extent was MEASURED as `[data_offset,
    ///   successor_offset)`, bounding to the authoritative uncompressed
    ///   data-section length for the last partition. This is the normal value:
    ///   NEITHER Cassandra 5.0 index format records a per-partition size (a BIG
    ///   index entry is `[key][data_offset vint][promoted_index_len vint]
    ///   [promoted_index]` —
    ///   `docs/sstables-definitive-guide/chapters/06-index-and-summary.md`, "Index.db
    ///   Entry Format" — and the BTI trie resolves an offset only), so the extent is
    ///   measured from index LAYOUT, the same bound the single-partition seek uses
    ///   to size its decompression window.
    /// - `"index"` — an SSTable reported a size directly in its index metadata
    ///   (`PartitionLoc.data_size`). Unreachable for Cassandra-written SSTables per
    ///   the above; retained so a producer that genuinely knows a size is never
    ///   forced to report a measured one.
    /// - `"unavailable"` — no authoritative extent at all, so the access contributes
    ///   ZERO bytes and is counted here instead. A size is never estimated,
    ///   interpolated by proportion, or defaulted (no-heuristics #28): the
    ///   incompleteness is published as a ratio rather than absorbed.
    ///
    /// Where an access mixes provenances the WEAKEST is reported — a total is only
    /// as well-founded as its weakest component.
    pub const SIZE_SOURCE: &str = "cqlite.read.size_source";
}

/// `cqlite.read.rows` — counter `{row}`.
///
/// Rows a read DELIVERED (see `ReadOpMeter`). On the Flight k-way merge scan
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
/// Total bytes read from Data.db (post-decompression), once per chunk decode.
/// Bounded attributes: [`attr::COMPRESSION`] alone — the chunk plane knows no format (#1701).
pub const READ_BYTES: &str = "cqlite.read.bytes";

/// `cqlite.read.partitions` — counter `{partition}`.
///
/// Partitions a read DELIVERED rows from on the CORE path (boundaries come from EMITTED
/// row keys, so a wholly tombstoned/TTL-expired partition contributes ZERO), and
/// partitions SCANNED on Flight's k-way merge arm — the gap is exactly the
/// fully-suppressed partitions, and `ReadOpMeter::record_row` records why. On that
/// Flight merge scan (#2162) the delta is emitted incrementally at a bounded row
/// threshold, so the counter climbs before the scan returns; the total is unchanged,
/// and like [`READ_ROWS`] it is FORMAT-AGNOSTIC (no attributes) while a single-SSTable
/// caller may attach [`attr::SSTABLE_FORMAT`]. Bounded: that OR none.
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

/// `cqlite.read.bti.rows_root_rejected` — counter `{partition}` (issue #3002).
///
/// Incremented once per clustering-slice read that could NOT use a BTI partition's
/// `Rows.db` row index because the row-index ROOT the `TrieIndexEntry` resolved to
/// failed structural validation, so the read decoded the WHOLE partition instead
/// (correct rows, no narrowing). Bounded attribute:
/// [`attr::ROWS_ROOT_REJECT_REASON`] (the closed
/// `RowsTrieRootRejectReason::label()` set).
///
/// Zero on a healthy table. A non-zero value names the cause of otherwise
/// unexplained clustering-read latency: every slice over the affected partitions is
/// doing a full-partition decode. The known producer is a `Rows.db` written by
/// CQLite <= 0.16 (mis-based root delta) — re-flush/re-compact those tables.
pub const READ_BTI_ROWS_ROOT_REJECTED: &str = "cqlite.read.bti.rows_root_rejected";

/// `cqlite.read.partition_access.distinct_partitions` — counter `{partition}`
/// (issue #2827).
///
/// Number of DISTINCT partitions that fell into each repeat-access bucket over
/// one closed measurement window of the bounded partition access-distribution
/// probe. Bounded attributes: [`attr::REPEAT_BUCKET`] (six labels) and
/// [`attr::SIZE_SOURCE`] (three labels) — eighteen series, fixed forever, regardless
/// of how many partitions the workload touched. Across all SEVEN
/// `cqlite.read.partition_access.*` metrics the budget is
/// `6x3 + 6 + 6 + 1 + 1 + 1 + 1 = 34`.
///
/// This is the concentration SHAPE of a keyed workload: a distribution
/// concentrated in `1` is a uniform (cache-hostile) access pattern; mass in
/// `9-16`/`17+` is a hot set. It is emitted ONCE per closed window and only when
/// the window recorded at least one access — a `0/0` emission would be a series
/// with no subject. The probe is OFF by default
/// (`CQLITE_PARTITION_ACCESS_PROBE`), so this metric is absent unless an
/// operator turned it on.
pub const READ_PARTITION_ACCESS_DISTINCT_PARTITIONS: &str =
    "cqlite.read.partition_access.distinct_partitions";

/// `cqlite.read.partition_access.accesses` — counter `1` (issue #2827).
///
/// Sum of the repeat counts of the partitions in each bucket, over one closed
/// window. Bounded attribute: [`attr::REPEAT_BUCKET`].
///
/// Emitting the access total PER BUCKET (rather than one grand total) removes
/// the within-bucket mean from any hit-ratio arithmetic: the open-ended `17+`
/// bucket stops being unbounded and the cache-sizing bound becomes a point value
/// rather than an interval. Paired with
/// [`READ_PARTITION_ACCESS_DISTINCT_PARTITIONS`], `accesses − distinct` is the
/// number of accesses a clairvoyant cache holding that bucket would have served.
pub const READ_PARTITION_ACCESS_ACCESSES: &str = "cqlite.read.partition_access.accesses";

/// `cqlite.read.partition_access.bytes` — counter `By` (issue #2827).
///
/// Sum of DISTINCT-partition on-disk bytes in each bucket over one closed
/// window. Bounded attribute: [`attr::REPEAT_BUCKET`]. A partition accessed ten
/// times contributes its size ONCE — the working set is defined over distinct
/// partitions.
///
/// Bytes come from each partition's MEASURED on-disk extent — its successor gap,
/// bounding to the authoritative uncompressed data-section length for the last
/// partition — never from an estimate. These are UNCOMPRESSED offsets, which is the
/// correct input for a decoded-size multiplier. An access with no authoritative
/// extent contributes ZERO here and is counted under
/// `distinct_partitions{cqlite.read.size_source="unavailable"}`, so an
/// incomplete byte total always has a visible `unavailable` series beside it.
pub const READ_PARTITION_ACCESS_BYTES: &str = "cqlite.read.partition_access.bytes";

/// `cqlite.read.partition_access.sample_denominator` — gauge `1` (issue #2827).
///
/// The sampling scale in force when the window closed: `1` means a CENSUS (every
/// distinct partition touched in the window was counted exactly); `2^k` means the
/// recorder hash-prefix-downsampled `k` times to stay inside its fixed-memory
/// table, so `distinct_partitions` and `bytes` are a 1-in-`2^k` sample of the
/// distinct-partition population (bucket FRACTIONS remain unbiased — the
/// admission predicate is a function of the key hash alone and is therefore
/// independent of a key's access frequency). No attributes.
pub const READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR: &str =
    "cqlite.read.partition_access.sample_denominator";

/// `cqlite.read.partition_access.dropped_accesses` — counter `1` (issue #2827).
///
/// Accesses the probe was asked to record but could NOT seat in its fixed counting
/// table, summed over closed windows. No attributes.
///
/// **Zero on a healthy window, and a non-zero value invalidates the window.** Only
/// keys NOT already in the table can be dropped, so a loss suppresses the
/// singleton bucket and OVERSTATES concentration — the direction that flatters a
/// cache. The decision procedure refuses any window reporting a non-zero value.
/// Exported (rather than only returned from `close_window`) so an operator reading
/// dashboards alone can tell a lossy window from a clean one.
pub const READ_PARTITION_ACCESS_DROPPED: &str = "cqlite.read.partition_access.dropped_accesses";

/// `cqlite.read.partition_access.sampling_floor` — gauge `1` (issue #2827).
///
/// `1` when the closed window reached the sampling-prefix cap, `0` otherwise. No
/// attributes.
///
/// A window at the floor is a ~1-in-a-million sample and is statistically
/// worthless; the decision procedure refuses it. Paired with
/// [`READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR`] this is the whole trustworthiness
/// signal for a window, readable without calling into the process.
///
/// Like [`READ_PARTITION_ACCESS_WINDOW_DROPPED`], this gauge is written only by the
/// NEWEST closed window: emission happens after the recorder's lock is released, so a
/// stale emit is skipped by close sequence rather than allowed to overwrite a newer
/// window's value.
pub const READ_PARTITION_ACCESS_SAMPLING_FLOOR: &str =
    "cqlite.read.partition_access.sampling_floor";

/// `cqlite.read.partition_access.window_dropped_accesses` — gauge `1` (issue #2827).
///
/// Accesses the LAST CLOSED window could not seat, reset every window. No
/// attributes.
///
/// Exists because its cumulative sibling [`READ_PARTITION_ACCESS_DROPPED`] cannot
/// answer "was THIS window clean": a counter that ever incremented reads non-zero
/// for the life of the process, so an instantaneous read of it can only say "this
/// process has lost input at some point". The spec requires that a consumer reading
/// the emitted series ALONE distinguish a lossy or floored window from a clean one,
/// and that needs a per-window signal with the same reset semantics as
/// [`READ_PARTITION_ACCESS_SAMPLING_FLOOR`].
///
/// A window is CLEAN exactly when this gauge and
/// [`READ_PARTITION_ACCESS_SAMPLING_FLOOR`] both read `0`. Both are emitted on every
/// closed window, including at zero, so absence is never ambiguous.
///
/// **"LAST CLOSED" is enforced, not incidental.** Windows close atomically but are
/// emitted after the recorder's lock is released, so an older window's emit can
/// arrive after a newer one's. Each closed window carries a monotonic close sequence
/// and the emit path skips this gauge for a stale sequence — without that, a late
/// emit would leave it describing the older window and invert the property. The
/// sequencing state is PER-RECORDER and its comparison is ATOMIC with the gauge
/// writes: a process-wide mark would let independent recorders suppress each other's
/// gauges, and deciding admission without holding it through the write would let an
/// admitted older emitter be preempted and then overwrite a newer one. The cumulative
/// [`READ_PARTITION_ACCESS_DROPPED`] counter is additive and is emitted regardless of
/// order.
pub const READ_PARTITION_ACCESS_WINDOW_DROPPED: &str =
    "cqlite.read.partition_access.window_dropped_accesses";

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
/// **WRITE-SIDE ONLY.** Per-chunk compression ratio (compressed bytes /
/// uncompressed bytes; ≤1.0 means the chunk shrank), recorded by the SSTable
/// *writer* as it compresses each chunk. There is NO read-side emission: the read
/// path decompresses chunks without recording a ratio, so this histogram says
/// nothing about the compression of the SSTables being read (issue #1705, AI5 of
/// epic #1686 — the previous wording implied a read-side series that does not
/// exist).
///
/// Scope note (#1406): the only emission site is `CompressedDataWriter`, and the
/// production write surface emits UNCOMPRESSED SSTables, so this series is silent
/// outside compressed-fixture synthesis. Bounded attributes:
/// [`attr::COMPRESSION`].
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

/// `cqlite.merge.active_merges` — gauge `{merge}` (issue #2765).
///
/// Live count of concurrent k-way MERGE operations (compaction, full-scan, and
/// channel-backed point-read fail-safe merges), incremented once per
/// `KWayMerger` that opens at least one egress channel and decremented once when
/// it is dropped. This is the divisor of the adaptive egress budget: the
/// per-channel `sync_channel` capacity every new merge receives is
/// `clamp(EGRESS_ROW_BUDGET / active_merges, MIN_CAP, 256)`, so this gauge makes
/// the otherwise-invisible backpressure throttle legible — a level well above
/// `EGRESS_ROW_BUDGET / 256` means concurrent merges are being squeezed toward
/// `MIN_CAP`. Deliberately DISTINCT from [`MERGE_PRODUCER_THREADS`], which
/// counts per-SOURCE producer threads (`O(K × active_merges)`): this counts
/// MERGES, the unit the budget is keyed on. No high-cardinality attributes.
pub const MERGE_ACTIVE_MERGES: &str = "cqlite.merge.active_merges";

/// `cqlite.merge.egress_channel_depth` — gauge `{entry}` (issue #2419, WS2).
///
/// Live occupancy of the bounded producer→consumer `sync_channel` (capacity up
/// to `STREAMING_CHANNEL_CAPACITY` = 256, adaptively reduced under concurrent
/// merges — see [`MERGE_ACTIVE_MERGES`] / issue #2765, `merge/mod.rs`) that carries merged
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

/// `cqlite.flight.tables_discovered` — gauge `{entry}` (issue #2684).
///
/// Number of `<keyspace>/<table>` SSTable directories currently VISIBLE under
/// the server's `--data-dir`, re-sampled on the same ~2s saturation tick as the
/// `cqlite.proc.*` gauges. Each tick does a readdir-only walk (keyspace dirs →
/// table dirs), counting a table dir iff it directly contains a `*-Data.db`
/// entry (name check only — NO stat-for-generation, NO open, NO parse), so the
/// cold-start invariant (#2385) holds: sampling never increments
/// [`INDEX_PARSES_TOTAL`]. `snapshots/` and `backups/` subtrees are excluded and
/// UUID-suffixed table dirs are counted correctly.
///
/// **Healthy vs alarming**: RISES when a new table appears on disk (new table, a
/// fixed mount) and FALLS when a table is dropped/removed; a wrong or empty
/// `--data-dir` reads **0** immediately, surfacing an inert mount before the
/// first query errors lazily. **Undersampling caveat (#2661)**: sampled every
/// ~2s, so a Prometheus scrape at a longer interval can miss a short-lived
/// transition — same caveat as the rest of the #2419 saturation family. No
/// high-cardinality attributes (total-only; the per-keyspace breakdown lives in
/// the one-time startup log line, never as a metric label).
pub const FLIGHT_TABLES_DISCOVERED: &str = "cqlite.flight.tables_discovered";

/// `cqlite.flight.warm_tables` — gauge `{entry}` (issue #2684).
///
/// The count of tables with a live (non-empty) warm reader set in the flight
/// `WarmTableRegistry` (a retired table leaves a zero-reader entry until the next
/// rebuild, so the reading filters those out rather than using the raw map size).
/// Atomic-backed at the registry's
/// mutation sites (independent of the sampler cadence): the post-mutation
/// `.len()` is emitted while the registry lock is held at the `rebuild()` insert
/// and `evict_to_budget()` removal, so the remove-then-reinsert transient the
/// rebuild performs never dips the reading.
///
/// **Healthy vs alarming**: RISES on the first serve of a previously-unseen
/// table (a `do_get` rebuild insert) and FALLS on eviction/retirement (budget
/// LRU or generation turnover). A level pinned at capacity with steady eviction
/// churn means the warm byte budget is small versus the working set. No
/// high-cardinality attributes (total-only). DISTINCT from
/// [`FLIGHT_TABLES_DISCOVERED`] (what is VISIBLE on disk): this is what is
/// actually OPENED and served warm.
pub const FLIGHT_WARM_TABLES: &str = "cqlite.flight.warm_tables";

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

/// Arrow Flight gRPC metric names (`cqlite.rpc.*`, `cqlite.warm.cache.*`,
/// `cqlite.flight.admission.*`) live in a sibling file so `catalog.rs` stays
/// inside the campsite-rule source target (#1116, split by #1707). Re-exported so
/// every public path (`catalog::RPC_DURATION`, …) is unchanged.
#[path = "catalog_flight.rs"]
mod flight;

pub use flight::{
    FLIGHT_ADMISSION_IN_USE, FLIGHT_ADMISSION_LIMIT, FLIGHT_ADMISSION_REJECTED_TOTAL,
    FLIGHT_ADMISSION_WAITING, FLIGHT_ADMISSION_WAIT_SECONDS, RPC_BYTES, RPC_DURATION,
    RPC_IN_FLIGHT, RPC_PHASE_ACTIVE, RPC_PHASE_DURATION, RPC_REQUESTS, RPC_ROWS, WARM_CACHE_EVICTS,
    WARM_CACHE_HITS, WARM_CACHE_MISSES, WARM_CACHE_REFRESH,
};

/// Metric-name registry tables (`ALL_METRICS`, `SATURATION_GAUGES`,
/// `ADMISSION_METRICS`, `STATS_ONLY_METRICS`) live in a sibling file so
/// `catalog.rs` stays inside the campsite-rule source target (#1116). Re-exported
/// so every public path (`catalog::ALL_METRICS`, …) is unchanged.
#[path = "catalog_registry.rs"]
mod registry;

pub use registry::{
    StatsOnlyMetric, ADMISSION_METRICS, ALL_METRICS, SATURATION_GAUGES, STATS_ONLY_METRICS,
};

/// Catalog invariant tests live in a sibling file so `catalog.rs` stays inside
/// the campsite-rule source target (#1116); they are logically the `tests`
/// submodule of this module.
#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
