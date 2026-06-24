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
    /// Bytes.
    pub const BYTES: &str = "By";
    /// Seconds (OTel prefers base-unit seconds for durations).
    pub const SECONDS: &str = "s";
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
}

/// `cqlite.read.rows` — counter `{row}`.
///
/// Total rows materialised by the read path. Bounded attributes:
/// [`attr::SSTABLE_FORMAT`].
pub const READ_ROWS: &str = "cqlite.read.rows";

/// `cqlite.read.bytes` — counter `By`.
///
/// Total bytes read from Data.db (post-decompression). Bounded attributes:
/// [`attr::SSTABLE_FORMAT`], [`attr::COMPRESSION`].
pub const READ_BYTES: &str = "cqlite.read.bytes";

/// `cqlite.read.partitions` — counter `{partition}`.
///
/// Total partitions scanned. Bounded attributes: [`attr::SSTABLE_FORMAT`].
pub const READ_PARTITIONS: &str = "cqlite.read.partitions";

/// `cqlite.read.duration` — histogram `s`.
///
/// Distribution of single read/scan operation durations in seconds. Bounded
/// attributes: [`attr::SSTABLE_FORMAT`].
pub const READ_DURATION: &str = "cqlite.read.duration";

/// `cqlite.query.duration` — histogram `s`.
///
/// Distribution of end-to-end query execution durations in seconds. Bounded
/// attributes: [`attr::SUBSYSTEM`]. NEVER attach the query text.
pub const QUERY_DURATION: &str = "cqlite.query.duration";

/// `cqlite.query.rows` — counter `{row}`.
///
/// Total rows returned to callers by the query engine. No high-cardinality
/// attributes.
pub const QUERY_ROWS: &str = "cqlite.query.rows";

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

/// `cqlite.compaction.tombstones_purged` — counter `{row}`.
///
/// Total tombstones purged (gc_grace / overlap-safe) during compaction. No
/// high-cardinality attributes.
pub const COMPACTION_TOMBSTONES_PURGED: &str = "cqlite.compaction.tombstones_purged";

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

/// `cqlite.errors.total` — counter `{error}`.
///
/// Total errors observed, the canonical error-rate signal (issue #1038).
/// Bounded attributes: [`attr::ERROR_CATEGORY`] and [`attr::SUBSYSTEM`] ONLY.
/// The raw error message is never attached.
pub const ERRORS_TOTAL: &str = "cqlite.errors.total";

/// All catalog metric names, for tests and registration sanity checks.
pub const ALL_METRICS: &[&str] = &[
    READ_ROWS,
    READ_BYTES,
    READ_PARTITIONS,
    READ_DURATION,
    QUERY_DURATION,
    QUERY_ROWS,
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
    COMPACTION_LAG,
    COMPACTION_FINALIZE_DURATION,
    COMPACTION_BUDGET_REQUESTED,
    COMPACTION_BUDGET_CONSUMED,
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
        ] {
            assert!(key.starts_with("cqlite."), "attr {key} must be namespaced");
        }
    }
}
