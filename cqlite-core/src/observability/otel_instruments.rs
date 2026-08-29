//! Cached OpenTelemetry instrument construction for the metric catalog.
//!
//! Split out of `otel.rs` to keep that file inside the campsite-rule source target
//! (#1116). This module owns the instrument set and its one-time build; `otel.rs`
//! keeps the record-routing (`add_counter` / `record_histogram` / `record_gauge`)
//! and the three resolvers the emit path calls.
//!
//! # A name/instrument mismatch is UNREPRESENTABLE here (issue #1705, roborev F3)
//!
//! Registration used to state each metric name TWICE — once in a builder call in
//! this file, once in a hand-written `catalog::NAME => &i.field` match arm in
//! `otel.rs`. Two statements of one fact can disagree, and a mis-wired arm
//! (`catalog::READ_ROWS => &i.read_bytes`) satisfied every guard we had — both the
//! structural parse and the runtime resolution only ever asked whether SOME
//! instrument existed for a name, never whether it was the RIGHT one, while
//! emissions landed under the wrong series.
//!
//! So the name is now stated ONCE. Each [`Registry`] method takes the catalog name
//! as a single parameter and uses it BOTH as the OTel instrument name AND as the
//! map key the resolver looks up, in the same expression. There is no second place
//! to write the name and therefore nothing to disagree with: the resolvers in
//! `otel.rs` are map lookups with no per-metric code at all. `catalog_tests.rs`
//! keeps that property by REDDING on any hand-written `catalog::IDENT =>` dispatch
//! arm in the resolvers (see
//! `a_handwritten_dispatch_arm_is_rejected_because_it_can_mis_wire`).
//!
//! **Both files are scanned** by the catalog's registration guards: any catalog
//! metric name registered here must appear in `ALL_METRICS`, and any `ALL_METRICS`
//! entry must be registered here or declared `catalog::STATS_ONLY_METRICS`. The
//! extractor reads the registration CALLS below (comments stripped first) and fails
//! CLOSED on an argument shape it does not recognise, so a registration written
//! with a string literal or a local alias reds the guard instead of vanishing from
//! it (issue #1705, roborev F4).

use super::catalog;
use super::otel::meter;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use std::collections::HashMap;
use std::sync::OnceLock;

/// Lazily-built, cached instruments for every catalog metric, keyed by the catalog
/// metric name they were CONSTRUCTED with.
///
/// Building an instrument on each record call is wasteful (re-registration overhead
/// and possible duplicate-instrument churn), so all catalog instruments are
/// constructed once and reused. Non-catalog names resolve to `None` and the emit
/// path falls back to an ad-hoc instrument so call sites never silently drop data.
///
/// The fields are maps rather than one field per metric precisely so that lookup
/// cannot name a different metric than construction did — see the module doc. The
/// emit path therefore pays one hash per record instead of walking the retired
/// match's up-to-45 `&str` comparisons; both are noise beside the SDK's own record
/// path (which takes a lock and aggregates), and the previous cost was already
/// O(arms) for every metric declared late in the match.
pub(super) struct Instruments {
    pub(super) counters: HashMap<&'static str, Counter<u64>>,
    pub(super) histograms: HashMap<&'static str, Histogram<f64>>,
    pub(super) gauges: HashMap<&'static str, Gauge<i64>>,
}

/// Builds an [`Instruments`] set, binding each catalog name to its instrument.
///
/// Each method mentions its `name` parameter twice — as the OTel instrument name
/// and as the map key — but it is ONE value from ONE call site, so the two cannot
/// diverge. This is the whole mechanism by which a mis-wire is unrepresentable.
struct Registry<'m> {
    meter: &'m Meter,
    counters: HashMap<&'static str, Counter<u64>>,
    histograms: HashMap<&'static str, Histogram<f64>>,
    gauges: HashMap<&'static str, Gauge<i64>>,
}

impl<'m> Registry<'m> {
    fn new(meter: &'m Meter) -> Self {
        Self {
            meter,
            counters: HashMap::new(),
            histograms: HashMap::new(),
            gauges: HashMap::new(),
        }
    }

    /// Register a `u64` counter for the catalog metric `name`.
    fn counter(&mut self, name: &'static str, unit: &'static str, description: &'static str) {
        let instrument = self
            .meter
            .u64_counter(name)
            .with_unit(unit)
            .with_description(description)
            .build();
        self.counters.insert(name, instrument);
    }

    /// Register an `f64` histogram for the catalog metric `name`.
    fn histogram(&mut self, name: &'static str, unit: &'static str, description: &'static str) {
        let instrument = self
            .meter
            .f64_histogram(name)
            .with_unit(unit)
            .with_description(description)
            .build();
        self.histograms.insert(name, instrument);
    }

    /// Register an `i64` gauge for the catalog metric `name`.
    fn gauge(&mut self, name: &'static str, unit: &'static str, description: &'static str) {
        let instrument = self
            .meter
            .i64_gauge(name)
            .with_unit(unit)
            .with_description(description)
            .build();
        self.gauges.insert(name, instrument);
    }

    fn build(self) -> Instruments {
        Instruments {
            counters: self.counters,
            histograms: self.histograms,
            gauges: self.gauges,
        }
    }
}

/// Build a FRESH instrument set bound to `meter`, touching no process-global state.
///
/// The production path calls this once, through [`instruments`], with the global
/// [`meter`]. Tests call it with a meter they own, so the registration guards in
/// `otel_tests.rs` can measure the registration table WITHOUT resolving the
/// `INSTRUMENTS`/`METER` `OnceLock`s (issue #1705): both are one-shot, so the first
/// caller in a process permanently binds the global meter — a guard that called
/// `instruments()` while no meter provider was installed would bind it to the NO-OP
/// provider and blind every later `testing::metrics_capture()` test in the same
/// binary. Supplying the meter removes that ordering hazard rather than ordering
/// around it.
///
/// The instrument set is identical either way: the same three `register_*` passes
/// run over it, so a guard reading an isolated set reads exactly the registrations
/// the emit path resolves.
pub(super) fn build_instruments(meter: &Meter) -> Instruments {
    let mut reg = Registry::new(meter);
    register_counters(&mut reg);
    register_histograms(&mut reg);
    register_gauges(&mut reg);
    reg.build()
}

pub(super) fn instruments() -> &'static Instruments {
    static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| build_instruments(meter()))
}

/// Counter registrations. One call per metric; the name is the map key.
fn register_counters(reg: &mut Registry<'_>) {
    reg.counter(
        catalog::READ_ROWS,
        catalog::unit::ROWS,
        "Rows a read delivered to its consumer.",
    );
    reg.counter(
        catalog::READ_BYTES,
        catalog::unit::BYTES,
        "Total bytes read from Data.db (post-decompression).",
    );
    reg.counter(
        catalog::READ_PARTITIONS,
        catalog::unit::PARTITIONS,
        "Total partitions scanned.",
    );
    reg.counter(
        catalog::READ_PARTITION_LOOKUP,
        catalog::unit::DIMENSIONLESS,
        "Total partition point lookups, keyed by {result, lookup_route, format}.",
    );
    reg.counter(
        catalog::READ_BLOOM_CHECKS,
        catalog::unit::DIMENSIONLESS,
        "Total bloom/BTI-trie presence checks, keyed by {result}.",
    );
    reg.counter(
        catalog::READ_SCAN_WINDOW_REFILL,
        catalog::unit::DIMENSIONLESS,
        "Windowed scan refills at compression-chunk boundaries.",
    );
    reg.counter(
        catalog::READ_SSTABLES_PRUNED,
        catalog::unit::SSTABLES,
        "SSTables skipped by a presence-oracle negative, keyed by {format}.",
    );
    reg.counter(
        catalog::READ_BLOOM_FALSE_NEGATIVES,
        catalog::unit::DIMENSIONLESS,
        "Opt-in presence-oracle false negatives (soundness alarm), keyed by {format}.",
    );
    reg.counter(
        catalog::READ_BTI_ROWS_ROOT_REJECTED,
        catalog::unit::PARTITIONS,
        "Clustering reads that decoded a whole BTI partition because its Rows.db \
                     row-index root failed validation, keyed by {reason} (#3002).",
    );
    reg.counter(
        catalog::READ_PARTITION_ACCESS_DISTINCT_PARTITIONS,
        catalog::unit::PARTITIONS,
        "Distinct partitions per repeat-access bucket (#2827).",
    );
    reg.counter(
        catalog::READ_PARTITION_ACCESS_ACCESSES,
        catalog::unit::DIMENSIONLESS,
        "Accesses per repeat-access bucket (#2827).",
    );
    reg.counter(
        catalog::READ_PARTITION_ACCESS_BYTES,
        catalog::unit::BYTES,
        "Distinct-partition on-disk bytes per bucket (#2827).",
    );
    reg.counter(
        catalog::READ_PARTITION_ACCESS_DROPPED,
        catalog::unit::DIMENSIONLESS,
        "Probe accesses that could not be seated (#2827).",
    );
    reg.counter(
        catalog::MERGE_ROWS_IN,
        catalog::unit::ROWS,
        "Rows consumed at the k-way merge reconcile boundary.",
    );
    reg.counter(
        catalog::MERGE_ROWS_OUT,
        catalog::unit::ROWS,
        "Rows emitted by the k-way merge reconcile boundary.",
    );
    reg.counter(
        catalog::QUERY_DEGRADED_PATH,
        catalog::unit::DIMENSIONLESS,
        "SELECTs taking a soundness fallback, keyed by {fallback_reason}.",
    );
    reg.counter(
        catalog::INDEX_PARSES_TOTAL,
        catalog::unit::DIMENSIONLESS,
        "Full Index.db partition-index parses (#2383 spin probe).",
    );
    reg.counter(
        catalog::INDEX_INTERVAL_PARSES_TOTAL,
        catalog::unit::DIMENSIONLESS,
        "Bounded Summary-guided Index.db interval parses, per point lookup (issue #2412).",
    );
    reg.counter(
        catalog::STORAGE_OPEN_SSTABLES,
        catalog::unit::SSTABLES,
        "SSTables discovered and opened, summed across opens.",
    );
    reg.counter(
        catalog::STORAGE_OPEN_BYTES,
        catalog::unit::BYTES,
        "On-disk Data.db bytes across SSTables discovered at open.",
    );
    reg.counter(
        catalog::STORAGE_OPEN_TABLES,
        catalog::unit::DIMENSIONLESS,
        "Logical tables represented by SSTables discovered at open.",
    );
    reg.counter(
        catalog::QUERY_ROWS,
        catalog::unit::ROWS,
        "Total rows returned to callers by the query engine.",
    );
    reg.counter(
        catalog::QUERY_ROWS_SCANNED,
        catalog::unit::ROWS,
        "Rows examined by SELECT scan before filtering/projection.",
    );
    reg.counter(
        catalog::ERRORS_TOTAL,
        catalog::unit::ERRORS,
        "Total errors observed, keyed by bounded {category, subsystem}.",
    );
    reg.counter(
        catalog::WRITE_MUTATIONS,
        catalog::unit::ROWS,
        "Mutations accepted by the write path.",
    );
    reg.counter(
        catalog::FLUSH_ROWS,
        catalog::unit::ROWS,
        "Rows flushed from memtable to L0 SSTables.",
    );
    reg.counter(
        catalog::FLUSH_BYTES,
        catalog::unit::BYTES,
        "Data.db bytes produced by memtable flushes.",
    );
    reg.counter(
        catalog::FLUSH_SSTABLES,
        catalog::unit::SSTABLES,
        "L0 SSTables created by memtable flushes.",
    );
    reg.counter(
        catalog::WRITE_PARTITIONS,
        catalog::unit::PARTITIONS,
        "Partitions written by the SSTable writer.",
    );
    reg.counter(
        catalog::WRITE_BYTES,
        catalog::unit::BYTES,
        "Data.db bytes produced by the SSTable writer.",
    );
    reg.counter(
        catalog::COMPACTION_ROWS_MERGED,
        catalog::unit::ROWS,
        "Rows emitted by compaction merge.",
    );
    reg.counter(
        catalog::COMPACTION_BYTES_WRITTEN,
        catalog::unit::BYTES,
        "Bytes written to compaction output SSTables.",
    );
    reg.counter(
        catalog::COMPACTION_SSTABLES_IN,
        catalog::unit::SSTABLES,
        "Input SSTables consumed by compactions.",
    );
    reg.counter(
        catalog::COMPACTION_SSTABLES_OUT,
        catalog::unit::SSTABLES,
        "Output SSTables produced by compactions.",
    );
    reg.counter(
        catalog::COMPACTION_TOMBSTONES_PURGED,
        catalog::unit::TOMBSTONES,
        "Tombstones genuinely purged during compaction.",
    );
    reg.counter(
        catalog::COMPACTION_TOMBSTONES_SUPPRESSED,
        catalog::unit::TOMBSTONES,
        "Live cells/rows shadowed by a tombstone during reconciliation.",
    );
    reg.counter(
        catalog::COMPACTION_TOMBSTONES_EMITTED,
        catalog::unit::TOMBSTONES,
        "Tombstone markers retained into the merge output.",
    );
    reg.counter(
        catalog::RPC_REQUESTS,
        catalog::unit::DIMENSIONLESS,
        "Arrow Flight RPC requests served.",
    );
    reg.counter(
        catalog::RPC_ROWS,
        catalog::unit::ROWS,
        "Rows returned to Flight clients.",
    );
    reg.counter(
        catalog::RPC_BYTES,
        catalog::unit::BYTES,
        "Record-batch payload bytes streamed to Flight clients.",
    );
    reg.counter(
        catalog::WARM_CACHE_HITS,
        catalog::unit::DIMENSIONLESS,
        "Flight warm-handle cache hits (#2310).",
    );
    reg.counter(
        catalog::WARM_CACHE_MISSES,
        catalog::unit::DIMENSIONLESS,
        "Flight warm-handle cache misses (#2310).",
    );
    reg.counter(
        catalog::WARM_CACHE_EVICTS,
        catalog::unit::DIMENSIONLESS,
        "Warm generations evicted (LRU / removed on disk) (#2310).",
    );
    reg.counter(
        catalog::WARM_CACHE_REFRESH,
        catalog::unit::DIMENSIONLESS,
        "Warm-handle refresh outcomes, keyed by {refresh_outcome} (#2310).",
    );
    reg.counter(
        catalog::FLIGHT_ADMISSION_REJECTED_TOTAL,
        catalog::unit::DIMENSIONLESS,
        "do_get requests rejected on admission timeout (#2420).",
    );
}

/// Histogram registrations.
fn register_histograms(reg: &mut Registry<'_>) {
    reg.histogram(
        catalog::READ_DURATION,
        catalog::unit::SECONDS,
        "Single read/scan operation duration in seconds.",
    );
    reg.histogram(
        catalog::QUERY_DURATION,
        catalog::unit::SECONDS,
        "End-to-end query execution duration in seconds.",
    );
    reg.histogram(
        catalog::COMPACTION_DURATION,
        catalog::unit::SECONDS,
        "Compaction run duration in seconds.",
    );
    reg.histogram(
        catalog::WAL_SYNC_DURATION,
        catalog::unit::SECONDS,
        "WAL fsync duration in seconds.",
    );
    reg.histogram(
        catalog::FLUSH_DURATION,
        catalog::unit::SECONDS,
        "Memtable-to-SSTable flush duration in seconds.",
    );
    reg.histogram(
        catalog::COMPRESSION_RATIO,
        catalog::unit::DIMENSIONLESS,
        "Per-chunk compression ratio.",
    );
    reg.histogram(
        catalog::COMPACTION_FINALIZE_DURATION,
        catalog::unit::SECONDS,
        "Compaction finalize duration in seconds.",
    );
    reg.histogram(
        catalog::COMPACTION_BUDGET_REQUESTED,
        catalog::unit::SECONDS,
        "Maintenance budget requested in seconds.",
    );
    reg.histogram(
        catalog::COMPACTION_BUDGET_CONSUMED,
        catalog::unit::SECONDS,
        "Maintenance budget consumed in seconds.",
    );
    reg.histogram(
        catalog::RPC_DURATION,
        catalog::unit::SECONDS,
        "Arrow Flight RPC handler duration in seconds.",
    );
    reg.histogram(
        catalog::RPC_PHASE_DURATION,
        catalog::unit::SECONDS,
        "do_get per-phase duration in seconds (#2162).",
    );
    reg.histogram(
        catalog::FLIGHT_ADMISSION_WAIT_SECONDS,
        catalog::unit::SECONDS,
        "do_get admission acquire wait time in seconds (#2420).",
    );
}

/// Gauge registrations.
fn register_gauges(reg: &mut Registry<'_>) {
    reg.gauge(
        catalog::READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR,
        catalog::unit::DIMENSIONLESS,
        "Probe sampling scale at window close; 1 = census.",
    );
    reg.gauge(
        catalog::READ_PARTITION_ACCESS_SAMPLING_FLOOR,
        catalog::unit::DIMENSIONLESS,
        "1 when the probe window hit its sampling cap (#2827).",
    );
    reg.gauge(
        catalog::READ_PARTITION_ACCESS_WINDOW_DROPPED,
        catalog::unit::DIMENSIONLESS,
        "Accesses the last closed probe window lost (#2827).",
    );
    reg.gauge(
        catalog::SSTABLES_OPEN,
        catalog::unit::SSTABLES,
        "Number of SSTables currently held open.",
    );
    reg.gauge(
        catalog::MEMTABLE_SIZE_BYTES,
        catalog::unit::BYTES,
        "Approximate active memtable size in bytes.",
    );
    reg.gauge(
        catalog::MEMTABLE_ROWS,
        catalog::unit::ROWS,
        "Rows currently buffered in the active memtable.",
    );
    reg.gauge(
        catalog::COMPACTION_LAG,
        catalog::unit::SSTABLES,
        "Current L0 SSTables pending compaction.",
    );
    reg.gauge(
        catalog::RPC_IN_FLIGHT,
        catalog::unit::DIMENSIONLESS,
        "Arrow Flight RPCs currently being handled.",
    );
    reg.gauge(
        catalog::RPC_PHASE_ACTIVE,
        catalog::unit::DIMENSIONLESS,
        "do_get phase currently executing (#2361).",
    );
    reg.gauge(
        catalog::MERGE_PRODUCER_THREADS,
        catalog::unit::THREADS,
        "Live k-way merge producer threads (#2316).",
    );
    reg.gauge(
        catalog::FLIGHT_ADMISSION_LIMIT,
        catalog::unit::DIMENSIONLESS,
        "Configured do_get admission ceiling K (#2420).",
    );
    reg.gauge(
        catalog::FLIGHT_ADMISSION_IN_USE,
        catalog::unit::DIMENSIONLESS,
        "do_get admission permits currently held (#2420).",
    );
    reg.gauge(
        catalog::FLIGHT_ADMISSION_WAITING,
        catalog::unit::DIMENSIONLESS,
        "do_get requests parked waiting for an admission permit (#2420).",
    );
    reg.gauge(
        catalog::MERGE_EGRESS_CHANNEL_DEPTH,
        catalog::unit::ENTRIES,
        "Live occupancy of the bounded merge egress sync_channel (#2419).",
    );
    reg.gauge(
        catalog::MERGE_ACTIVE_MERGES,
        catalog::unit::MERGES,
        "Live concurrent k-way merges — divisor of the adaptive egress budget (#2765).",
    );
    reg.gauge(
        catalog::PROC_THREADS,
        catalog::unit::THREADS,
        "Process OS thread count (/proc/self/task, Linux) (#2419).",
    );
    reg.gauge(
        catalog::PROC_FDS,
        catalog::unit::FDS,
        "Process open fd count (/proc/self/fd, Linux) (#2419).",
    );
    reg.gauge(
        catalog::PROC_RSS_BYTES,
        catalog::unit::BYTES,
        "Process resident set size (/proc/self/status VmRSS, Linux) (#2419).",
    );
    reg.gauge(
        catalog::FLIGHT_BLOCKING_TASKS_IN_USE,
        catalog::unit::THREADS,
        "Flight spawn_blocking tasks currently outstanding (#2419).",
    );
    reg.gauge(
        catalog::FLIGHT_TABLES_DISCOVERED,
        catalog::unit::ENTRIES,
        "Table dirs visible under --data-dir, sampled by readdir on the ~2s tick (#2684).",
    );
    reg.gauge(
        catalog::FLIGHT_WARM_TABLES,
        catalog::unit::ENTRIES,
        "Tables with a live warm reader set in the registry (#2684).",
    );
}
