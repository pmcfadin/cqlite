//! Metric-name registry tables — the grouped `&[&str]` lists over the metric-name
//! constants declared in [`super`].
//!
//! Split out of `catalog.rs` mechanically (issue #1705, campsite rule #1116): the
//! constants and their operator documentation stay there, the *registry tables*
//! live here. Every table is re-exported from [`super`], so the public paths
//! (`observability::catalog::ALL_METRICS`, `…::SATURATION_GAUGES`,
//! `…::ADMISSION_METRICS`) are unchanged and no caller moves.

use super::*;

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
    // BTI row-index root rejection → full-partition fallback (#3002)
    READ_BTI_ROWS_ROOT_REJECTED,
    // Bounded partition access-distribution probe (#2827), default-OFF
    READ_PARTITION_ACCESS_DISTINCT_PARTITIONS,
    READ_PARTITION_ACCESS_ACCESSES,
    READ_PARTITION_ACCESS_BYTES,
    READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR,
    READ_PARTITION_ACCESS_DROPPED,
    READ_PARTITION_ACCESS_SAMPLING_FLOOR,
    READ_PARTITION_ACCESS_WINDOW_DROPPED,
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
    MERGE_ACTIVE_MERGES,
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
    // Flight table-visibility gauges (#2684)
    FLIGHT_TABLES_DISCOVERED,
    FLIGHT_WARM_TABLES,
];

/// The saturation gauges added by issue #2419 (WS2), extended by the #2684
/// flight table-visibility gauges. Grouped for the distinctness/registration
/// tests and #2426's operator reference so they can be presented as one section
/// without re-listing them by hand.
pub const SATURATION_GAUGES: &[&str] = &[
    MERGE_EGRESS_CHANNEL_DEPTH,
    MERGE_ACTIVE_MERGES,
    PROC_THREADS,
    PROC_FDS,
    PROC_RSS_BYTES,
    FLIGHT_BLOCKING_TASKS_IN_USE,
    // Flight table-visibility gauges (#2684): sampler-driven tables_discovered +
    // atomic-backed warm_tables. Grouped here so the dedicated-otel-arm +
    // namespaced/unique tests cover them.
    FLIGHT_TABLES_DISCOVERED,
    FLIGHT_WARM_TABLES,
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
/// A catalogued metric that is deliberately **NOT** registered as a live OTel
/// instrument, carried together with the AFFIRMATIVE evidence that it *is*
/// nevertheless surfaced on a stats path (issue #1705).
///
/// A bare name list would be an unguarded waiver list: appending a name to it is
/// all it would take to silence the registration-completeness guard for a metric
/// whose instrument someone genuinely forgot to wire. So an entry is not a name, it
/// is a name plus a **probe** — a function that READS the metric's value out of a
/// [`MemoryStats`](crate::memory::MemoryStats) snapshot. That makes the exemption
/// positively justified twice over:
///
/// * the COMPILER refuses the entry unless the field it names exists, and
/// * `stats_only_probes_read_distinct_live_stats_fields` executes every probe
///   against a snapshot with a unique sentinel per field, so a probe that reads
///   nothing, returns a constant, or duplicates another entry's field FAILS.
///
/// A metric that is not actually on the stats path has no such field, so it cannot
/// be exempted by declaration alone.
pub struct StatsOnlyMetric {
    /// The catalogued metric name (must be in [`ALL_METRICS`]).
    pub name: &'static str,
    /// Human-readable `Database::stats()` path, for failure messages and operators.
    pub stats_field: &'static str,
    /// Reads this metric's value out of a real stats snapshot.
    pub stats_probe: fn(&crate::memory::MemoryStats) -> u64,
}

/// Catalogued metrics that are deliberately **NOT** registered as live OTel
/// instruments (issue #1705, AI5 of epic #1686 "observability honesty").
///
/// The catalog is the operator-facing name registry — [`super::super::operator_docs`]
/// generates the metrics reference from [`ALL_METRICS`] — and a few of those names
/// are surfaced ONLY through the in-process `Database::stats()` snapshot, never
/// through an OTel meter, so they are not scrapeable from Prometheus / an OTel
/// collector. That is a real and legitimate state, but it is indistinguishable
/// from the bug the registration-completeness guard exists to catch (a catalogued
/// name whose instrument nobody ever wired) unless it is DECLARED — and declared
/// with evidence, per [`StatsOnlyMetric`].
///
/// This is the single source every half derives from:
/// `every_catalogued_metric_is_otel_registered_or_declared_stats_only` treats a
/// name here as exempt; `stats_only_declaration_matches_the_operator_docs` requires
/// each one's [`super::super::operator_docs`] annotation to carry the "not emitted
/// as a live OTel instrument" disclosure, so the machine-checkable list and the
/// operator-facing prose cannot drift apart;
/// `stats_only_metrics_are_catalogued_and_never_otel_registered` fails if a name
/// listed here DOES get an instrument, so a stale exemption cannot silently weaken
/// the guard; and `stats_only_probes_read_distinct_live_stats_fields` fails unless
/// every entry's probe really reads its own field of a live snapshot.
///
/// Adding an entry here is a claim-boundary decision, not a formality: it says
/// "this metric is not scrapeable". Wire the instrument instead where you can.
pub const STATS_ONLY_METRICS: &[StatsOnlyMetric] = &[
    // Issue #2059: the process-global key→partition-offset cache reports through
    // `Database::stats().memory_stats`, not an OTel meter.
    StatsOnlyMetric {
        name: KEY_CACHE_HITS,
        stats_field: "memory_stats.key_cache_hits",
        stats_probe: |s| s.key_cache_hits,
    },
    StatsOnlyMetric {
        name: KEY_CACHE_MISSES,
        stats_field: "memory_stats.key_cache_misses",
        stats_probe: |s| s.key_cache_misses,
    },
    StatsOnlyMetric {
        name: KEY_CACHE_EVICTIONS,
        stats_field: "memory_stats.key_cache_evictions",
        stats_probe: |s| s.key_cache_evictions,
    },
    StatsOnlyMetric {
        name: KEY_CACHE_INVALIDATIONS,
        stats_field: "memory_stats.key_cache_invalidations",
        stats_probe: |s| s.key_cache_invalidations,
    },
    StatsOnlyMetric {
        name: KEY_CACHE_RESIDENT_BYTES,
        stats_field: "memory_stats.key_cache_resident_bytes",
        stats_probe: |s| s.key_cache_resident_bytes as u64,
    },
    StatsOnlyMetric {
        name: KEY_CACHE_CAPACITY_BYTES,
        stats_field: "memory_stats.key_cache_capacity_bytes",
        stats_probe: |s| s.key_cache_capacity_bytes as u64,
    },
];
