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
/// `full_scan`, ~1 for a `partition_lookup`. Bounded attributes:
/// [`attr::ACCESS_PATH`]. Emitted by the modern `SelectExecutor` only.
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
    READ_PARTITION_LOOKUP,
    READ_BLOOM_CHECKS,
    STORAGE_OPEN_SSTABLES,
    STORAGE_OPEN_BYTES,
    STORAGE_OPEN_TABLES,
    QUERY_DURATION,
    QUERY_ROWS,
    QUERY_ROWS_SCANNED,
    SSTABLES_OPEN,
    COMPACTION_DURATION,
    ERRORS_TOTAL,
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
        ] {
            assert!(key.starts_with("cqlite."), "attr {key} must be namespaced");
        }
    }
}
