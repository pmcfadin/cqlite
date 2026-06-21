//! Statistics.db writer - writes SSTable metadata
//!
//! Generates the Statistics.db component with min/max timestamps, TTL, and
//! other metadata used for delta encoding in Data.db.
//!
//! Critical requirements:
//! - MUST be written BEFORE Data.db (provides delta encoding baseline)
//! - Min timestamp, max timestamp
//! - Min TTL, max TTL
//! - Min local deletion time, max local deletion time
//! - Partition count, row count
//!
//! # Statistics.db Format (Cassandra 5.0 Compatible)
//!
//! This implementation produces a full Cassandra 5.0 nb-format Statistics.db with:
//! - TOC (Table of Contents) with checksums
//! - Four metadata components: VALIDATION, COMPACTION, STATS, HEADER
//! - Per-component CRC32 checksums
//! - Global CRC32 checksum validation
//!
//! ## Format Structure
//!
//! ```text
//! [0-3]   num_components (u32 BE) = 4
//! [4-7]   CRC32(num_components)
//! [8-39]  TOC entries (4 components × 8 bytes each):
//!           [u32 BE] component_type (MetadataType ordinal)
//!           [u32 BE] component_offset
//! [40-43] CRC32(num_components + all TOC entries) [cumulative]
//! [44+]   Component data:
//!           [N bytes] component_data
//!           [4 bytes] CRC32(component_data)
//!           ... (repeated for each component)
//! ```
//!
//! ## MetadataType Component IDs
//!
//! From Cassandra's `MetadataType.java` enum (ordinal values):
//! - 0: VALIDATION (validator class name)
//! - 1: COMPACTION (compaction metadata)
//! - 2: STATS (statistics including EncodingStats)
//! - 3: SERIALIZATION_HEADER (table schema)

use crate::error::{Error, Result};
use crate::parser::vint::encode_vuint;
use crate::schema::TableSchema;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;

/// Maximum number of bins in the tombstone drop-time histogram.
///
/// Mirrors Cassandra's `StreamingTombstoneHistogramBuilder.MAX_BIN_SIZE = 100`.
const TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE: i32 = 100;

/// Streaming tombstone drop-time histogram.
///
/// Mirrors Cassandra's `StreamingTombstoneHistogramBuilder` with a fixed maximum
/// of 100 bins. Tombstone local-deletion-times are accumulated; when the bin count
/// exceeds the maximum the two nearest bins are merged, keeping the bin count at or
/// below `MAX_BIN_SIZE`.
///
/// Serialisation uses the **legacy** (nb/mc) format:
/// ```text
/// i32 BE  maxBinSize  (100 when non-empty, 0 when empty)
/// i32 BE  size        (actual number of bins)
/// for each bin:
///   f64 BE  point  (local-deletion-time as a double)
///   i64 BE  value  (count of tombstones in this bin)
/// ```
#[derive(Debug, Clone, Default)]
pub struct TombstoneHistogram {
    /// Bins keyed by `local_deletion_time as i64` (for stable ordering).
    /// Each value is the count of tombstones that map to this bin.
    bins: BTreeMap<i64, i64>,
}

impl TombstoneHistogram {
    /// Create an empty histogram.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a tombstone with the given local-deletion-time (Unix seconds).
    ///
    /// If adding the new point would exceed `MAX_BIN_SIZE` the two closest
    /// existing bins are merged first, exactly as Cassandra's builder does.
    pub fn update(&mut self, local_deletion_time: i32) {
        let key = local_deletion_time as i64;
        *self.bins.entry(key).or_insert(0) += 1;

        // Merge bins while over capacity
        while self.bins.len() as i32 > TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE {
            self.merge_closest_bins();
        }
    }

    /// Return `true` if no tombstone deletion times have been recorded.
    pub fn is_empty(&self) -> bool {
        self.bins.is_empty()
    }

    /// Return the number of populated bins.
    pub fn size(&self) -> i32 {
        self.bins.len() as i32
    }

    /// Merge the two bins whose keys are closest together.
    ///
    /// The merged bin's key is the weighted average of the two points;
    /// its value is the sum of both counts.  This matches Cassandra's
    /// `mergeNearestBins` approach.
    fn merge_closest_bins(&mut self) {
        if self.bins.len() < 2 {
            return;
        }

        // Find the pair of adjacent keys with the smallest gap.
        let keys: Vec<i64> = self.bins.keys().copied().collect();
        let mut min_gap = i64::MAX;
        let mut merge_idx = 0usize;
        for i in 0..keys.len() - 1 {
            let gap = keys[i + 1] - keys[i];
            if gap < min_gap {
                min_gap = gap;
                merge_idx = i;
            }
        }

        let k1 = keys[merge_idx];
        let k2 = keys[merge_idx + 1];
        let v1 = self.bins.remove(&k1).unwrap_or(0);
        let v2 = self.bins.remove(&k2).unwrap_or(0);
        let total = v1 + v2;
        // Weighted average key (same formula as Cassandra)
        let merged_key = if total == 0 {
            (k1 + k2) / 2
        } else {
            (k1 * v1 + k2 * v2) / total
        };
        *self.bins.entry(merged_key).or_insert(0) += total;
    }

    /// Iterate over bins in ascending key order as `(point_as_f64, count)` pairs.
    fn entries(&self) -> impl Iterator<Item = (f64, i64)> + '_ {
        self.bins.iter().map(|(&k, &v)| (k as f64, v))
    }
}

/// Epoch constants for EncodingStats (from Cassandra's EncodingStats.java)
/// These are used to compute deltas from a baseline for more compact encoding
const TIMESTAMP_EPOCH: i64 = 1442880000000000; // Sept 22, 2015 00:00:00 UTC in microseconds
const DELETION_TIME_EPOCH: i32 = 1442880000; // Sept 22, 2015 00:00:00 UTC in seconds
const TTL_EPOCH: i32 = 0; // TTL epoch is 0 (no offset)

/// Number of metadata components in Statistics.db
/// Cassandra 5.0 nb-format has 4 components: VALIDATION, COMPACTION, STATS, HEADER
const NUM_COMPONENTS: u32 = 4;

/// MetadataType ordinal values (from Cassandra's MetadataType.java enum)
const METADATA_TYPE_VALIDATION: u32 = 0;
const METADATA_TYPE_COMPACTION: u32 = 1;
const METADATA_TYPE_STATS: u32 = 2;
const METADATA_TYPE_SERIALIZATION_HEADER: u32 = 3;

/// Statistics metadata collected during memtable flush
///
/// This structure holds all the metadata needed to write Statistics.db.
/// Values are collected as rows are written to Data.db.
#[derive(Debug, Clone)]
pub struct StatisticsMetadata {
    /// Minimum timestamp in the SSTable (microseconds since epoch)
    pub min_timestamp: i64,
    /// Maximum timestamp in the SSTable (microseconds since epoch)
    pub max_timestamp: i64,
    /// Minimum local deletion time (seconds since epoch, for tombstones)
    pub min_local_deletion_time: i32,
    /// Maximum local deletion time (seconds since epoch)
    pub max_local_deletion_time: i32,
    /// Minimum TTL value (seconds, 0 if no TTL)
    pub min_ttl: i32,
    /// Maximum TTL value (seconds, 0 if no TTL)
    pub max_ttl: i32,
    /// Total number of partitions in the SSTable
    pub partition_count: u64,
    /// Total number of rows (live + tombstones)
    pub row_count: u64,
    /// Total number of columns across all rows
    pub column_count: u64,
    /// Total size of all rows in bytes
    pub total_rows_size: u64,
    /// Histogram of tombstone local-deletion-times for `estimatedTombstoneDropTime`.
    ///
    /// Populated by `update_local_deletion_time`; serialised into Statistics.db
    /// so Cassandra can compute `estimatedDroppableTombstoneRatio` and schedule
    /// tombstone compaction.  Uses the same streaming builder algorithm as
    /// Cassandra's `StreamingTombstoneHistogramBuilder` (max 100 bins).
    pub tombstone_histogram: TombstoneHistogram,
    /// First (lowest-token) decorated partition-key bytes written to this
    /// SSTable, or `None` if no partition has been written. Serialised as the
    /// `da`-format `StatsMetadata.firstKey` (BtiFormat `hasKeyRange`); tracked in
    /// token order by `SSTableWriter::write_partition`.
    pub first_key: Option<Vec<u8>>,
    /// Last (highest-token) decorated partition-key bytes written to this
    /// SSTable. Serialised as the `da`-format `StatsMetadata.lastKey`.
    pub last_key: Option<Vec<u8>>,
    /// Whether any partition written to this SSTable carries a partition-level
    /// deletion (partition tombstone).
    ///
    /// Serialised as the `da`-format `StatsMetadata.hasPartitionLevelDeletions`
    /// boolean (`hasPartitionLevelDeletionsPresenceMarker`); written as a single
    /// `writeBoolean` byte (`0x01` true, `0x00` false) — matching
    /// cassandra-5.0.0 `StatsMetadata.StatsMetadataSerializer.serialize`
    /// (line 495). Set by `SSTableWriter::write_partition` whenever a mutation
    /// contributes a `partition_tombstone`. Ignored by the legacy BIG (nb/oa)
    /// STATS body, which never serialises this field.
    pub has_partition_level_deletions: bool,
}

impl Default for StatisticsMetadata {
    fn default() -> Self {
        Self {
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            min_local_deletion_time: i32::MAX,
            max_local_deletion_time: i32::MIN,
            min_ttl: i32::MAX,
            max_ttl: 0,
            partition_count: 0,
            row_count: 0,
            column_count: 0,
            total_rows_size: 0,
            tombstone_histogram: TombstoneHistogram::new(),
            first_key: None,
            last_key: None,
            has_partition_level_deletions: false,
        }
    }
}

impl StatisticsMetadata {
    /// Create a new empty statistics metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Update timestamp range with a new timestamp value.
    ///
    /// LIVE deletion markers must never enter the min/max aggregates (issue
    /// #851, Cassandra `d5bc7fb5`). Cassandra encodes `DeletionTime.LIVE` with
    /// `markedForDeleteAt = Long.MIN_VALUE`, and a `NO_DELETION` / absent
    /// liveness timestamp surfaces as `Long.MAX_VALUE`. Folding either sentinel
    /// into `minTimestamp` / `maxTimestamp` would poison the stats, so they are
    /// skipped here at the single aggregation chokepoint.
    pub fn update_timestamp(&mut self, timestamp: i64) {
        if Self::is_live_timestamp(timestamp) {
            return;
        }
        self.min_timestamp = self.min_timestamp.min(timestamp);
        self.max_timestamp = self.max_timestamp.max(timestamp);
    }

    /// Update local deletion time range (for tombstones) and the drop-time histogram.
    ///
    /// Call this for every tombstone local-deletion-time encountered while writing:
    /// cell tombstones, row deletions, range tombstones, and partition tombstones.
    ///
    /// A LIVE marker (`DeletionTime.LIVE.localDeletionTime == Integer.MAX_VALUE`,
    /// the same value as `Cell.NO_DELETION_TIME`) is not a tombstone: it must not
    /// pull `minLocalDeletionTime` down to the sentinel nor inflate the tombstone
    /// drop-time histogram (issue #851, Cassandra `d5bc7fb5`).
    pub fn update_local_deletion_time(&mut self, deletion_time: i32) {
        if Self::is_live_local_deletion_time(deletion_time) {
            return;
        }
        self.min_local_deletion_time = self.min_local_deletion_time.min(deletion_time);
        self.max_local_deletion_time = self.max_local_deletion_time.max(deletion_time);
        self.tombstone_histogram.update(deletion_time);
    }

    /// True when `timestamp` is a LIVE / `NO_DELETION` marker rather than a real
    /// deletion timestamp. CQLite writes `DeletionTime.LIVE` as `Long.MIN_VALUE`
    /// (see `data_writer.rs`); Cassandra's `NO_DELETION` / `NO_TIMESTAMP` sentinel
    /// is `Long.MAX_VALUE`. Both mean "no deletion" and must be excluded from
    /// timestamp aggregation.
    fn is_live_timestamp(timestamp: i64) -> bool {
        timestamp == i64::MIN || timestamp == i64::MAX
    }

    /// True when `deletion_time` is a LIVE marker (`Integer.MAX_VALUE`) rather
    /// than a real tombstone local-deletion-time.
    fn is_live_local_deletion_time(deletion_time: i32) -> bool {
        deletion_time == i32::MAX
    }

    /// Update TTL range
    pub fn update_ttl(&mut self, ttl: i32) {
        if ttl > 0 {
            self.min_ttl = self.min_ttl.min(ttl);
            self.max_ttl = self.max_ttl.max(ttl);
        }
    }

    /// Increment partition count
    pub fn increment_partition_count(&mut self) {
        self.partition_count += 1;
    }

    /// Record a partition key in the SSTable key range.
    ///
    /// Partitions are written in ascending token order (enforced by
    /// `SSTableWriter::write_partition`), so the FIRST key seen is the lowest and
    /// the LAST is the highest. Used to populate the `da`-format
    /// `StatsMetadata.firstKey`/`lastKey` (`hasKeyRange`). The clone is bounded by
    /// the partition-key size (typically tens of bytes), not the partition data.
    pub fn update_key_range(&mut self, key: &[u8]) {
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.last_key = Some(key.to_vec());
    }

    /// Record that a partition-level deletion (partition tombstone) was written.
    ///
    /// Once set, this stays `true` for the lifetime of the SSTable and drives the
    /// `da`-format `StatsMetadata.hasPartitionLevelDeletions` marker. Called by
    /// `SSTableWriter::write_partition` for any partition carrying a
    /// `partition_tombstone`.
    pub fn mark_partition_level_deletion(&mut self) {
        self.has_partition_level_deletions = true;
    }

    /// Increment row count
    pub fn increment_row_count(&mut self) {
        self.row_count += 1;
    }

    /// Add to column count
    pub fn add_column_count(&mut self, count: u64) {
        self.column_count += count;
    }

    /// Add to total rows size
    pub fn add_rows_size(&mut self, size: u64) {
        self.total_rows_size += size;
    }

    /// Finalize metadata before writing (normalize sentinel values)
    pub fn finalize(&mut self) {
        // If no timestamps were recorded, set to 0
        if self.min_timestamp == i64::MAX {
            self.min_timestamp = 0;
        }
        if self.max_timestamp == i64::MIN {
            self.max_timestamp = 0;
        }

        // If no deletion times were recorded, set to 0
        if self.min_local_deletion_time == i32::MAX {
            self.min_local_deletion_time = 0;
        }
        if self.max_local_deletion_time == i32::MIN {
            self.max_local_deletion_time = 0;
        }

        // If no TTLs were recorded, set min_ttl to 0
        if self.min_ttl == i32::MAX {
            self.min_ttl = 0;
        }
    }
}

/// Convert a CQL type name to Cassandra internal marshal type.
///
/// This is the reverse of `convert_marshal_type_to_cql` in enhanced_statistics_parser.rs.
/// Used when writing the SERIALIZATION_HEADER component of Statistics.db.
///
/// Handles:
/// - Primitive types: text, int, bigint, uuid, etc.
/// - Collections: list<T>, set<T>, map<K,V>
/// - Frozen wrappers: frozen<list<T>>, frozen<map<K,V>>
/// - Tuples: tuple<T1, T2, ...>
fn cql_type_to_marshal_type(cql_type: &str) -> String {
    // Normalize to lowercase for case-insensitive matching.
    // CQL type names are case-insensitive, and the parser may preserve
    // original case from CQL files (e.g., "SET<TEXT>" instead of "set<text>").
    let trimmed = cql_type.trim().to_lowercase();
    let trimmed = trimmed.as_str();
    let prefix = "org.apache.cassandra.db.marshal.";

    // Handle parameterized types: list<T>, set<T>, map<K,V>, frozen<T>, tuple<T1,T2>
    if let Some(inner) = strip_cql_wrapper(trimmed, "list") {
        return format!("{prefix}ListType({})", cql_type_to_marshal_type(inner));
    }
    if let Some(inner) = strip_cql_wrapper(trimmed, "set") {
        return format!("{prefix}SetType({})", cql_type_to_marshal_type(inner));
    }
    if let Some(inner) = strip_cql_wrapper(trimmed, "map") {
        let args = split_cql_type_args(inner);
        if args.len() == 2 {
            return format!(
                "{prefix}MapType({},{})",
                cql_type_to_marshal_type(args[0]),
                cql_type_to_marshal_type(args[1])
            );
        }
        // Malformed map type — fall through to BytesType
    }
    if let Some(inner) = strip_cql_wrapper(trimmed, "frozen") {
        return format!("{prefix}FrozenType({})", cql_type_to_marshal_type(inner));
    }
    if let Some(inner) = strip_cql_wrapper(trimmed, "tuple") {
        let args = split_cql_type_args(inner);
        let components: Vec<String> = args.iter().map(|a| cql_type_to_marshal_type(a)).collect();
        return format!("{prefix}TupleType({})", components.join(","));
    }

    // Primitive types
    match trimmed {
        "text" | "varchar" => format!("{prefix}UTF8Type"),
        "int" => format!("{prefix}Int32Type"),
        "bigint" => format!("{prefix}LongType"),
        "smallint" => format!("{prefix}ShortType"),
        "tinyint" => format!("{prefix}ByteType"),
        "float" => format!("{prefix}FloatType"),
        "double" => format!("{prefix}DoubleType"),
        "boolean" => format!("{prefix}BooleanType"),
        "blob" => format!("{prefix}BytesType"),
        "uuid" => format!("{prefix}UUIDType"),
        "timeuuid" => format!("{prefix}TimeUUIDType"),
        "timestamp" => format!("{prefix}TimestampType"),
        "date" => format!("{prefix}SimpleDateType"),
        "time" => format!("{prefix}TimeType"),
        "duration" => format!("{prefix}DurationType"),
        "inet" => format!("{prefix}InetAddressType"),
        "ascii" => format!("{prefix}AsciiType"),
        "decimal" => format!("{prefix}DecimalType"),
        "varint" => format!("{prefix}IntegerType"),
        "counter" => format!("{prefix}CounterColumnType"),
        // Fallback: use BytesType for unknown types
        _ => format!("{prefix}BytesType"),
    }
}

/// Strip a CQL wrapper type like `list<inner>` and return the inner string.
/// Returns None if `cql_type` does not start with `wrapper<`.
fn strip_cql_wrapper<'a>(cql_type: &'a str, wrapper: &str) -> Option<&'a str> {
    let pattern = format!("{}<", wrapper);
    if let Some(rest) = cql_type.strip_prefix(&pattern) {
        // Find the matching closing '>' (handling nested angle brackets)
        let mut depth = 1;
        for (i, ch) in rest.char_indices() {
            match ch {
                '<' => depth += 1,
                '>' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(rest[..i].trim());
                    }
                }
                _ => {}
            }
        }
    }
    None
}

/// Split CQL type arguments at top-level commas (respecting nested angle brackets).
/// E.g. `"int, map<text, int>"` → `["int", "map<text, int>"]`
fn split_cql_type_args(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => {
                result.push(s[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = s[start..].trim();
    if !last.is_empty() {
        result.push(last);
    }
    result
}

/// Statistics.db component writer
///
/// Writes the Statistics.db file with metadata for SSTable delta encoding.
#[derive(Debug)]
pub struct StatisticsWriter {
    /// Path to the Statistics.db file to write
    path: PathBuf,
    /// Emit the Cassandra-canonical `da` (BtiFormat) `StatsMetadata` layout
    /// instead of the legacy `nb` layout.
    ///
    /// The `da` STATS component differs from `nb` in the fields gated by the
    /// BtiFormat version flags (all true for `da` except `hasLegacyMinMax`):
    /// `hasUIntDeletionTime`, `hasImprovedMinMax` (clusteringTypes + a covered
    /// `Slice` instead of legacy min/max value lists), `hasIsTransient`,
    /// `hasOriginatingHostId`, `hasPartitionLevelDeletionsPresenceMarker`,
    /// `hasKeyRange` (first/last key) and `hasTokenSpaceCoverage`. Cassandra's
    /// `sstabledump`/`sstablemetadata` deserialize a `da`-descriptor
    /// Statistics.db with this layout and reject the `nb` layout (a
    /// `Slice.<init>` assertion fires while reading `coveredClustering`).
    /// Authority: cassandra-5.0.0 `StatsMetadata.StatsMetadataSerializer` +
    /// `BtiFormat.BtiVersion` version flags.
    bti: bool,
}

impl StatisticsWriter {
    /// Create a new Statistics.db writer for the legacy `nb`/`oa` BIG layout.
    ///
    /// # Arguments
    /// * `path` - Path where Statistics.db will be written
    pub fn new(path: PathBuf) -> Self {
        Self { path, bti: false }
    }

    /// Create a new Statistics.db writer that emits the Cassandra-canonical `da`
    /// (BtiFormat) `StatsMetadata` layout.
    pub fn new_bti(path: PathBuf) -> Self {
        Self { path, bti: true }
    }

    /// Write Statistics.db file with the given metadata
    ///
    /// Generates a Cassandra 5.0 compatible Statistics.db file with full TOC structure:
    /// 1. TOC header with component count and checksums
    /// 2. VALIDATION component (validator class name)
    /// 3. COMPACTION component (minimal metadata)
    /// 4. STATS component (EncodingStats with baselines)
    /// 5. SERIALIZATION_HEADER component (schema-derived or minimal stub)
    ///
    /// Each component is followed by a CRC32 checksum for validation.
    ///
    /// # Arguments
    /// * `metadata` - Statistics metadata to write
    /// * `schema` - Optional table schema for populating serialization header
    ///
    /// # Returns
    /// `Ok(())` on success, or an error if writing fails
    pub fn write(&self, metadata: &StatisticsMetadata, schema: Option<&TableSchema>) -> Result<()> {
        let mut meta = metadata.clone();
        meta.finalize();

        // Build component data
        let validation_data = self.build_validation_component()?;
        let compaction_data = self.build_compaction_component()?;
        // The STATS body is version-gated: BTI (`da`) emits the BtiFormat
        // `StatsMetadata` layout (covered-clustering Slice, uint deletion times,
        // key range, token-space coverage); BIG (`nb`/`oa`) emits the legacy
        // layout. `schema` is needed for the `da` clustering-type list.
        let stats_data = if self.bti {
            self.build_stats_component_da(&meta, schema)?
        } else {
            self.build_stats_component(&meta)?
        };
        // Use pre-finalize metadata for the SerializationHeader EncodingStats.
        // The baselines in the header MUST match those used by the DataWriter for
        // delta encoding. The DataWriter uses the raw (pre-finalize) metadata values.
        let header_data = self.build_serialization_header_component(schema, metadata)?;

        // Calculate component offsets
        // TOC structure: 4 (count) + 4 (checksum) + (4*8) TOC entries + 4 (checksum) = 44 bytes
        let toc_size = 4 + 4 + (NUM_COMPONENTS as usize * 8) + 4;
        let mut offset = toc_size;

        let validation_offset = offset;
        offset += validation_data.len() + 4; // +4 for component checksum

        let compaction_offset = offset;
        offset += compaction_data.len() + 4;

        let stats_offset = offset;
        offset += stats_data.len() + 4;

        let header_offset = offset;
        // header_data has its own checksum at the end

        // Verify all offsets fit in u32 (Statistics.db should never exceed 4GB)
        if offset > u32::MAX as usize {
            return Err(Error::Storage(format!(
                "Statistics.db too large: {} bytes exceeds u32::MAX",
                offset
            )));
        }

        // Build the complete file
        let mut buffer = Vec::new();
        let mut crc = crc32fast::Hasher::new();

        // Write component count
        buffer.write_all(&NUM_COMPONENTS.to_be_bytes())?;
        self.update_checksum_int(&mut crc, NUM_COMPONENTS);

        // Write first checksum (after count)
        let checksum1 = crc.clone().finalize();
        buffer.write_all(&checksum1.to_be_bytes())?;

        // Reset CRC for TOC (we'll recompute cumulatively)
        crc = crc32fast::Hasher::new();
        self.update_checksum_int(&mut crc, NUM_COMPONENTS);

        // Write TOC entries (type, offset pairs)
        self.write_toc_entry(
            &mut buffer,
            &mut crc,
            METADATA_TYPE_VALIDATION,
            validation_offset as u32,
        )?;
        self.write_toc_entry(
            &mut buffer,
            &mut crc,
            METADATA_TYPE_COMPACTION,
            compaction_offset as u32,
        )?;
        self.write_toc_entry(
            &mut buffer,
            &mut crc,
            METADATA_TYPE_STATS,
            stats_offset as u32,
        )?;
        self.write_toc_entry(
            &mut buffer,
            &mut crc,
            METADATA_TYPE_SERIALIZATION_HEADER,
            header_offset as u32,
        )?;

        // Write TOC checksum (cumulative from count)
        let toc_checksum = crc.finalize();
        buffer.write_all(&toc_checksum.to_be_bytes())?;

        // Write components with per-component checksums
        self.write_component(&mut buffer, &validation_data)?;
        self.write_component(&mut buffer, &compaction_data)?;
        self.write_component(&mut buffer, &stats_data)?;
        self.write_component(&mut buffer, &header_data)?;

        // Write to file
        std::fs::write(&self.path, buffer).map_err(|e| {
            Error::Storage(format!(
                "Failed to write Statistics.db to {}: {}",
                self.path.display(),
                e
            ))
        })?;

        Ok(())
    }

    /// Update CRC32 checksum with a u32 value (big-endian)
    ///
    /// Mimics Java's FBUtilities.updateChecksumInt()
    fn update_checksum_int(&self, crc: &mut crc32fast::Hasher, value: u32) {
        crc.update(&value.to_be_bytes());
    }

    /// Write a TOC entry (component type and offset) with cumulative CRC update
    fn write_toc_entry(
        &self,
        buffer: &mut Vec<u8>,
        crc: &mut crc32fast::Hasher,
        component_type: u32,
        offset: u32,
    ) -> Result<()> {
        buffer.write_all(&component_type.to_be_bytes())?;
        self.update_checksum_int(crc, component_type);

        buffer.write_all(&offset.to_be_bytes())?;
        self.update_checksum_int(crc, offset);

        Ok(())
    }

    /// Write a component with its CRC32 checksum
    fn write_component(&self, buffer: &mut Vec<u8>, data: &[u8]) -> Result<()> {
        // Write component data
        buffer.write_all(data)?;

        // Write component checksum
        let checksum = crc32fast::hash(data);
        buffer.write_all(&checksum.to_be_bytes())?;

        Ok(())
    }

    /// Build VALIDATION component (MetadataType ordinal 0)
    ///
    /// Format (ValidationMetadata.java):
    /// - partitioner class name (Java writeUTF: u16 BE length + UTF-8 bytes)
    /// - bloom filter FP chance (f64 BE)
    fn build_validation_component(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // Partitioner class name (Java writeUTF format)
        let partitioner = b"org.apache.cassandra.dht.Murmur3Partitioner";

        // Java writeUTF: u16 BE length prefix + modified UTF-8 bytes
        let len = partitioner.len() as u16;
        buffer.write_all(&len.to_be_bytes())?;
        buffer.write_all(partitioner)?;

        // Bloom filter false positive chance (f64 BE)
        let fp_chance = 0.01f64;
        buffer.write_all(&fp_chance.to_be_bytes())?;

        Ok(buffer)
    }

    /// Build COMPACTION component (MetadataType ordinal 1)
    ///
    /// Format (CompactionMetadata.java):
    /// - cardinality estimator (i32 BE length + HyperLogLogPlus bytes)
    ///
    /// We write a minimal valid empty HyperLogLogPlus sketch.
    fn build_compaction_component(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // Cardinality estimator: ByteArrayUtil.writeWithLength(bytes, out)
        // Format: i32 BE length + data bytes
        //
        // Minimal valid HyperLogLogPlus(p=11, sp=25) in SPARSE format:
        // - 4 bytes: version (-2 as i32 = 0xFFFFFFFE)
        // - 1 byte: p = 11 (0x0B)
        // - 1 byte: sp = 25 (0x19)
        // - 1 byte: format type = SPARSE (0x01)
        // - 4 bytes: tempSetSize = 0
        // - 4 bytes: sparseSetSize = 0
        // Total: 15 bytes

        const HLL_DATA: [u8; 15] = [
            0xFF, 0xFF, 0xFF, 0xFE, // version = -2 (HyperLogLogPlus marker)
            0x0B, // p = 11 (precision)
            0x19, // sp = 25 (sparse precision)
            0x01, // format = SPARSE
            0x00, 0x00, 0x00, 0x00, // tempSetSize = 0
            0x00, 0x00, 0x00, 0x00, // sparseSetSize = 0
        ];

        // Write length prefix (i32 BE)
        buffer.write_all(&(HLL_DATA.len() as i32).to_be_bytes())?;

        // Write HLL data
        buffer.write_all(&HLL_DATA)?;

        Ok(buffer)
    }

    /// Build STATS component (MetadataType ordinal 2)
    ///
    /// Format for nb version (StatsMetadata.java lines 401-512):
    /// This is a complete serialization of all required fields for Cassandra 5.0 nb format.
    fn build_stats_component(&self, metadata: &StatisticsMetadata) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // 1-2. EstimatedHistogram estimatedPartitionSize and estimatedCellPerPartitionCount
        // Minimal valid histogram: size=2, one offset/count pair
        self.write_estimated_histogram(&mut buffer)?;
        self.write_estimated_histogram(&mut buffer)?;

        // 3. CommitLogPosition commitLogUpperBound (NONE = segmentId=-1, position=0)
        buffer.write_all(&(-1i64).to_be_bytes())?; // segmentId
        buffer.write_all(&0i32.to_be_bytes())?; // position

        // 4. long minTimestamp
        buffer.write_all(&metadata.min_timestamp.to_be_bytes())?;

        // 5. long maxTimestamp
        buffer.write_all(&metadata.max_timestamp.to_be_bytes())?;

        // 6. int minLocalDeletionTime (use Integer.MAX_VALUE if no deletions)
        let min_del_time = if metadata.min_local_deletion_time == 0 {
            i32::MAX
        } else {
            metadata.min_local_deletion_time
        };
        buffer.write_all(&min_del_time.to_be_bytes())?;

        // 7. int maxLocalDeletionTime
        let max_del_time = if metadata.max_local_deletion_time == 0 {
            i32::MAX
        } else {
            metadata.max_local_deletion_time
        };
        buffer.write_all(&max_del_time.to_be_bytes())?;

        // 8. int minTTL
        buffer.write_all(&metadata.min_ttl.to_be_bytes())?;

        // 9. int maxTTL
        buffer.write_all(&metadata.max_ttl.to_be_bytes())?;

        // 10. double compressionRatio (use -1.0 for unknown)
        buffer.write_all(&(-1.0f64).to_be_bytes())?;

        // 11. TombstoneHistogram estimatedTombstoneDropTime
        // Populated from tombstone local-deletion-times accumulated during the write path.
        // Legacy serializer (nb format): maxBinSize (i32), size (i32),
        // then size × (f64 point, i64 value).
        self.write_tombstone_histogram(&mut buffer, &metadata.tombstone_histogram)?;

        // 12. int sstableLevel
        buffer.write_all(&0i32.to_be_bytes())?;

        // 13. long repairedAt
        buffer.write_all(&0i64.to_be_bytes())?;

        // 14. int minClusteringCount (no clustering = 0)
        buffer.write_all(&0i32.to_be_bytes())?;

        // 15. [clustering values] - count=0 means no values to write

        // 16. int maxClusteringCount
        buffer.write_all(&0i32.to_be_bytes())?;

        // 17. [clustering values] - count=0 means no values to write

        // 18. boolean hasLegacyCounterShards
        buffer.write_all(&[0x00])?; // false

        // 19. long totalColumnsSet
        buffer.write_all(&metadata.column_count.to_be_bytes())?;

        // 20. long totalRows
        buffer.write_all(&metadata.row_count.to_be_bytes())?;

        // 21. CommitLogPosition commitLogLowerBound (NONE)
        buffer.write_all(&(-1i64).to_be_bytes())?; // segmentId
        buffer.write_all(&0i32.to_be_bytes())?; // position

        // 22. IntervalSet<CommitLogPosition> commitLogIntervals (empty set: size=0)
        buffer.write_all(&0i32.to_be_bytes())?;

        // 23. byte pendingRepair (0 = null, no pending repair)
        buffer.write_all(&[0x00])?;

        // 24. boolean isTransient
        buffer.write_all(&[0x00])?; // false

        // 25. byte originatingHostId (0 = null)
        buffer.write_all(&[0x00])?;

        Ok(buffer)
    }

    /// Build the STATS component for the Cassandra-canonical `da` (BtiFormat)
    /// layout.
    ///
    /// Field order and gating follow cassandra-5.0.0
    /// `StatsMetadata.StatsMetadataSerializer.serialize` evaluated for
    /// `BtiFormat.BtiVersion` (all version flags `true` except `hasLegacyMinMax`,
    /// which is `false`):
    ///
    /// 1.  estimatedPartitionSize (EstimatedHistogram)
    /// 2.  estimatedCellPerPartitionCount (EstimatedHistogram)
    /// 3.  commitLogUpperBound (CommitLogPosition)
    /// 4.  minTimestamp, maxTimestamp (long)
    /// 5.  min/maxLocalDeletionTime as **unsigned int** (`hasUIntDeletionTime`);
    ///     `NO_DELETION_TIME` (Long.MAX) maps to `0xFFFFFFFF`.
    /// 6.  minTTL, maxTTL (int)
    /// 7.  compressionRatio (double)
    /// 8.  estimatedTombstoneDropTime (TombstoneHistogram)
    /// 9.  sstableLevel (int), repairedAt (long)
    /// 10. improvedMinMax (`!hasLegacyMinMax && hasImprovedMinMax`):
    ///     clusteringTypes list + coveredClustering `Slice`. We emit
    ///     `Slice.ALL` (BOTTOM..TOP) — a valid, conservative covering slice that
    ///     Cassandra accepts and that matches the `Covered clusterings: [, ]`
    ///     shown by `sstablemetadata` on the real `da` fixtures.
    /// 11. hasLegacyCounterShards (bool)
    /// 12. totalColumnsSet, totalRows (long)
    /// 13. commitLogLowerBound (CommitLogPosition), commitLogIntervals (IntervalSet)
    /// 14. pendingRepair (byte 0 = null)
    /// 15. isTransient (bool)
    /// 16. originatingHostId (byte 0 = null)
    /// 17. hasPartitionLevelDeletions (bool)
    /// 18. firstKey, lastKey (vint-length ByteBuffer) — `hasKeyRange`
    /// 19. tokenSpaceCoverage (double) — `hasTokenSpaceCoverage`
    fn build_stats_component_da(
        &self,
        metadata: &StatisticsMetadata,
        schema: Option<&TableSchema>,
    ) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // 1-2. EstimatedHistogram estimatedPartitionSize / estimatedCellPerPartitionCount
        self.write_estimated_histogram(&mut buffer)?;
        self.write_estimated_histogram(&mut buffer)?;

        // 3. CommitLogPosition commitLogUpperBound (NONE)
        buffer.write_all(&(-1i64).to_be_bytes())?; // segmentId
        buffer.write_all(&0i32.to_be_bytes())?; // position

        // 4. minTimestamp, maxTimestamp
        buffer.write_all(&metadata.min_timestamp.to_be_bytes())?;
        buffer.write_all(&metadata.max_timestamp.to_be_bytes())?;

        // 5. min/maxLocalDeletionTime — unsigned int (hasUIntDeletionTime).
        // No tombstones (sentinel) → NO_DELETION_TIME_UNSIGNED_INTEGER = 0xFFFFFFFF;
        // otherwise the low 32 bits of the (seconds-since-epoch) deletion time
        // (Cassandra `CassandraUInt.fromLong` == `(int) value`).
        let min_ldt_uint: u32 = if metadata.min_local_deletion_time == 0 {
            0xFFFF_FFFF
        } else {
            metadata.min_local_deletion_time as u32
        };
        let max_ldt_uint: u32 = if metadata.max_local_deletion_time == 0 {
            0xFFFF_FFFF
        } else {
            metadata.max_local_deletion_time as u32
        };
        buffer.write_all(&min_ldt_uint.to_be_bytes())?;
        buffer.write_all(&max_ldt_uint.to_be_bytes())?;

        // 6. minTTL, maxTTL
        buffer.write_all(&metadata.min_ttl.to_be_bytes())?;
        buffer.write_all(&metadata.max_ttl.to_be_bytes())?;

        // 7. compressionRatio (-1.0 = unknown)
        buffer.write_all(&(-1.0f64).to_be_bytes())?;

        // 8. TombstoneHistogram estimatedTombstoneDropTime.
        //
        // The `da` (BtiFormat) version resolves `TombstoneHistogram.getSerializer`
        // to the modern `HistogramSerializer` (long point + int value per bin),
        // NOT the legacy serializer (double + long) used by older versions. Using
        // the legacy entry encoding here mis-sizes the body and derails the
        // subsequent `coveredClustering` Slice deserialization in Cassandra.
        self.write_tombstone_histogram_modern(&mut buffer, &metadata.tombstone_histogram)?;

        // 9. sstableLevel, repairedAt
        buffer.write_all(&0i32.to_be_bytes())?;
        buffer.write_all(&0i64.to_be_bytes())?;

        // 10. improvedMinMax: clusteringTypes list + coveredClustering Slice.
        //
        // AbstractTypeSerializer.serializeList: unsigned-VInt count, then each
        // type as an unsigned-VInt-length-prefixed UTF-8 marshal-class string —
        // the exact encoding used for the SERIALIZATION_HEADER clusteringTypes.
        let clustering_types: Vec<String> = schema
            .map(|s| {
                s.clustering_keys
                    .iter()
                    .map(|ck| cql_type_to_marshal_type(&ck.data_type))
                    .collect()
            })
            .unwrap_or_default();
        buffer.write_all(&encode_vuint(clustering_types.len() as u64))?;
        for ty in &clustering_types {
            buffer.write_all(&encode_vuint(ty.len() as u64))?;
            buffer.write_all(ty.as_bytes())?;
        }
        // coveredClustering = Slice.ALL = (BOTTOM, TOP). A ClusteringBound
        // serialises as `[byte kind ordinal][short size][values...]`; BOTTOM and
        // TOP are empty bounds (size 0). Kind ordinals (ClusteringPrefix.Kind):
        // INCL_START_BOUND = 1 (BOTTOM), INCL_END_BOUND = 6 (TOP).
        const KIND_INCL_START_BOUND: u8 = 1;
        const KIND_INCL_END_BOUND: u8 = 6;
        buffer.write_all(&[KIND_INCL_START_BOUND])?;
        buffer.write_all(&0u16.to_be_bytes())?; // start size = 0
        buffer.write_all(&[KIND_INCL_END_BOUND])?;
        buffer.write_all(&0u16.to_be_bytes())?; // end size = 0

        // 11. hasLegacyCounterShards
        buffer.write_all(&[0x00])?;

        // 12. totalColumnsSet, totalRows
        buffer.write_all(&metadata.column_count.to_be_bytes())?;
        buffer.write_all(&metadata.row_count.to_be_bytes())?;

        // 13. commitLogLowerBound (NONE) + commitLogIntervals (empty IntervalSet)
        buffer.write_all(&(-1i64).to_be_bytes())?; // segmentId
        buffer.write_all(&0i32.to_be_bytes())?; // position
        buffer.write_all(&0i32.to_be_bytes())?; // interval set size = 0

        // 14. pendingRepair (null)
        buffer.write_all(&[0x00])?;

        // 15. isTransient
        buffer.write_all(&[0x00])?;

        // 16. originatingHostId (null)
        buffer.write_all(&[0x00])?;

        // 17. hasPartitionLevelDeletions
        //
        // cassandra-5.0.0 StatsMetadata.StatsMetadataSerializer.serialize line 495:
        //   out.writeBoolean(component.hasPartitionLevelDeletions)
        // DataOutput.writeBoolean emits a single byte: 0x01 for true, 0x00 false.
        // Set when any partition written carries a partition-level tombstone.
        let has_partition_deletions = if metadata.has_partition_level_deletions {
            0x01u8
        } else {
            0x00u8
        };
        buffer.write_all(&[has_partition_deletions])?;

        // 18. firstKey, lastKey (ByteBufferUtil.writeWithVIntLength:
        // unsigned-VInt length then the raw key bytes). Empty when no partitions.
        let first = metadata.first_key.as_deref().unwrap_or(&[]);
        let last = metadata.last_key.as_deref().unwrap_or(&[]);
        buffer.write_all(&encode_vuint(first.len() as u64))?;
        buffer.write_all(first)?;
        buffer.write_all(&encode_vuint(last.len() as u64))?;
        buffer.write_all(last)?;

        // 19. tokenSpaceCoverage (double). Cassandra writes NaN when not computed
        // (sstablemetadata renders this as "Local token space coverage: NaN").
        buffer.write_all(&f64::NAN.to_be_bytes())?;

        Ok(buffer)
    }

    /// Write an EstimatedHistogram (EstimatedHistogram.java lines 414-429)
    ///
    /// Format:
    /// - int: bucket count (we use 2 for minimal valid histogram)
    /// - for each bucket: long offset + long count
    ///
    /// Minimal valid: 2 buckets (size-1=1 offset, size=2 counts)
    fn write_estimated_histogram(&self, buffer: &mut Vec<u8>) -> Result<()> {
        // Bucket count
        buffer.write_all(&2i32.to_be_bytes())?;

        // Bucket 0: offset=1, count=0
        buffer.write_all(&1i64.to_be_bytes())?; // offset
        buffer.write_all(&0i64.to_be_bytes())?; // count

        // Bucket 1: offset=1 (gets overwritten per spec), count=0
        buffer.write_all(&1i64.to_be_bytes())?; // offset (overwrite of offsets[0])
        buffer.write_all(&0i64.to_be_bytes())?; // count

        Ok(())
    }

    /// Serialise a `TombstoneHistogram` using Cassandra's legacy format (nb/mc).
    ///
    /// Binary layout (matches `TombstoneHistogram.LegacyHistogramSerializer`):
    /// ```text
    /// i32 BE  maxBinSize  — capacity: 100 when non-empty, 0 when empty
    /// i32 BE  size        — actual number of populated bins
    /// for each of the `size` bins (ascending point order):
    ///   f64 BE  point  — local-deletion-time bucket centre (as double)
    ///   i64 BE  value  — tombstone count for this bucket
    /// ```
    ///
    /// An empty histogram writes 8 bytes (`maxBinSize=0, size=0`), matching
    /// what Cassandra writes for SSTables with no tombstones.
    fn write_tombstone_histogram(
        &self,
        buffer: &mut Vec<u8>,
        histogram: &TombstoneHistogram,
    ) -> Result<()> {
        if histogram.is_empty() {
            // Empty: maxBinSize=0, size=0 — no entry pairs follow
            buffer.write_all(&0i32.to_be_bytes())?; // maxBinSize
            buffer.write_all(&0i32.to_be_bytes())?; // size
        } else {
            // Non-empty: maxBinSize=100 (the capacity constant), then the bins
            buffer.write_all(&TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE.to_be_bytes())?; // maxBinSize
            buffer.write_all(&histogram.size().to_be_bytes())?; // size
            for (point, value) in histogram.entries() {
                buffer.write_all(&point.to_be_bytes())?; // f64 BE point
                buffer.write_all(&value.to_be_bytes())?; // i64 BE value
            }
        }
        Ok(())
    }

    /// Serialise a `TombstoneHistogram` using Cassandra's modern (`oa`/`da`)
    /// `HistogramSerializer` format.
    ///
    /// Identical framing to the legacy serializer (`i32 maxBinSize`, `i32 size`)
    /// but each bin entry is `i64 BE point` + `i32 BE value` (12 bytes), where
    /// the legacy serializer used `f64 point` + `i64 value` (16 bytes). The
    /// version split: `StatsMetadata.StatsMetadataSerializer` resolves
    /// `TombstoneHistogram.getSerializer(version)` to the modern
    /// `HistogramSerializer` for `oa`+ (incl. `BtiFormat.BtiVersion` `da`) and
    /// the `LegacyHistogramSerializer` for older versions.
    ///
    /// Authority: repo `docs/sstables-definitive-guide/statistics-db-writer-spec.md`
    /// (TombstoneHistogram legacy vs modern note) and
    /// cassandra-5.0.0 `TombstoneHistogram.java` (`HistogramSerializer` ->
    /// `long` point + `int` value).
    ///
    /// ```text
    /// i32 BE  maxBinSize  — capacity: 100 when non-empty, 0 when empty
    /// i32 BE  size        — actual number of populated bins
    /// for each of the `size` bins (ascending point order):
    ///   i64 BE  point  — local-deletion-time bucket centre (as long)
    ///   i32 BE  value  — tombstone count for this bucket
    /// ```
    fn write_tombstone_histogram_modern(
        &self,
        buffer: &mut Vec<u8>,
        histogram: &TombstoneHistogram,
    ) -> Result<()> {
        if histogram.is_empty() {
            buffer.write_all(&0i32.to_be_bytes())?; // maxBinSize
            buffer.write_all(&0i32.to_be_bytes())?; // size
        } else {
            buffer.write_all(&TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE.to_be_bytes())?; // maxBinSize
            buffer.write_all(&histogram.size().to_be_bytes())?; // size
            for (point, value) in histogram.entries() {
                // Modern: long point (truncate the double bucket centre) + int value.
                buffer.write_all(&(point as i64).to_be_bytes())?; // i64 BE point
                buffer.write_all(&(value as i32).to_be_bytes())?; // i32 BE value
            }
        }
        Ok(())
    }

    /// Build SERIALIZATION_HEADER component (MetadataType ordinal 3)
    ///
    /// Format (SerializationHeader.java Serializer, lines 594-603):
    /// - EncodingStats: 3 unsigned VInts (minTimestamp, minLocalDeletionTime, minTTL deltas from epochs)
    /// - keyType: VInt length + UTF-8 type string
    /// - clusteringTypes: unsigned VInt count + list of types
    /// - staticColumns: unsigned VInt count + map of (column name, type)
    /// - regularColumns: unsigned VInt count + map of (column name, type)
    ///
    /// When `schema` is Some, populates keyType, clustering types, and column
    /// names/types from the actual table schema. When None, falls back to a
    /// minimal stub (BytesType, zero columns).
    ///
    /// # Column-set encoding (>64 columns)
    ///
    /// The static/regular column sets are encoded exactly as Cassandra's
    /// `SerializationHeader.Serializer.writeColumnsWithTypes`
    /// (cassandra-5.0.0 `SerializationHeader.java` lines 489-497): an unsigned-VInt
    /// column count followed by `count` `(VInt-length name, VInt-length marshal type)`
    /// pairs. This path has **no** 64-column limit and never uses a bitmap.
    ///
    /// The 64-bit bitmap encoding lives in `Columns.serializer.serializeSubset`
    /// (`Columns.java` lines 503-531) and is only used to serialise a per-row column
    /// subset against a pre-shared superset (Data.db rows / inter-node messaging),
    /// where `supersetCount < 64` selects the bitmap and `>= 64` switches to a VInt
    /// delta list. It is never used for the SSTable SERIALIZATION_HEADER, so wide
    /// tables (>64 columns) round-trip losslessly here.
    fn build_serialization_header_component(
        &self,
        schema: Option<&TableSchema>,
        metadata: &StatisticsMetadata,
    ) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // EncodingStats: 3 unsigned VInts representing deltas from epochs.
        // These baselines MUST match the values used by DataWriter for delta encoding.
        // Cassandra: EncodingStats.Serializer.serialize() writes:
        //   writeUnsignedVInt(minTimestamp - TIMESTAMP_EPOCH)
        //   writeUnsignedVInt(minLocalDeletionTime - DELETION_TIME_EPOCH)
        //   writeUnsignedVInt(minTTL - TTL_EPOCH)

        // minTimestamp delta from epoch
        let min_ts = if metadata.min_timestamp == i64::MAX {
            // No data recorded: use epoch as baseline
            TIMESTAMP_EPOCH as u64
        } else {
            metadata.min_timestamp as u64
        };
        let min_ts_delta = min_ts.wrapping_sub(TIMESTAMP_EPOCH as u64);
        buffer.write_all(&encode_vuint(min_ts_delta))?;

        // minLocalDeletionTime delta from epoch.
        //
        // Cassandra: `EncodingStats.Serializer.serialize` writes the local-deletion
        // baseline with `writeUnsignedVInt32(minLocalDeletionTime - DELETION_TIME_EPOCH)`
        // (both operands are Java `int`s), and the reader recovers it with
        // `readUnsignedVInt32()`, which runs `VIntCoding.checkedCast` and REJECTS any
        // decoded value that does not round-trip through a SIGNED 32-bit `int`
        // (`(int)value != value`). The on-disk form therefore carries the SIGN-EXTENDED
        // delta: for a small LDT like 2, `2 - DELETION_TIME_EPOCH` is the negative int
        // `-1442879998`, written as the sign-extended 64-bit VInt `0xFFFFFFFFAA…` which
        // `readUnsignedVInt32` accepts and folds back to `2`. Truncating to a bare `u32`
        // (e.g. `2852087298`) is OUT OF RANGE for `checkedCast` and makes Cassandra's
        // `sstabledump` reject the SSTable (verified live against `cassandra:5.0`).
        //
        // Casting the i32 delta to `u64` sign-extends in Rust exactly as Java's
        // `writeUnsignedVInt32` requires, and it also handles a far-future LDT stored as
        // a negative i32 bit pattern identically (the bit pattern IS the signed int the
        // reader expects). This mirrors the DataWriter per-row deletion deltas.
        let min_ldt = if metadata.min_local_deletion_time == i32::MAX {
            // No deletions: use Integer.MAX_VALUE as baseline (DeletionTime.LIVE)
            i32::MAX
        } else {
            metadata.min_local_deletion_time
        };
        let min_del_delta = (min_ldt.wrapping_sub(DELETION_TIME_EPOCH) as i64) as u64;
        buffer.write_all(&encode_vuint(min_del_delta))?;

        // minTTL delta from TTL_EPOCH (TTL_EPOCH=0)
        let min_ttl = if metadata.min_ttl == i32::MAX {
            // No TTL: use 0 as baseline
            0u64
        } else {
            metadata.min_ttl as u64
        };
        let min_ttl_delta = min_ttl.wrapping_sub(TTL_EPOCH as u64);
        buffer.write_all(&encode_vuint(min_ttl_delta))?;

        match schema {
            Some(s) => {
                // keyType: single PK → simple type, composite PK → CompositeType(...)
                let key_marshal = if s.partition_keys.len() > 1 {
                    let inner: Vec<String> = s
                        .partition_keys
                        .iter()
                        .map(|pk| cql_type_to_marshal_type(&pk.data_type))
                        .collect();
                    format!(
                        "org.apache.cassandra.db.marshal.CompositeType({})",
                        inner.join(",")
                    )
                } else if !s.partition_keys.is_empty() {
                    cql_type_to_marshal_type(&s.partition_keys[0].data_type)
                } else {
                    "org.apache.cassandra.db.marshal.BytesType".to_string()
                };
                buffer.write_all(&encode_vuint(key_marshal.len() as u64))?;
                buffer.write_all(key_marshal.as_bytes())?;

                // clusteringTypes: VUInt count + for each CK: VUInt-length-prefixed marshal type
                buffer.write_all(&encode_vuint(s.clustering_keys.len() as u64))?;
                for ck in &s.clustering_keys {
                    let ck_marshal = cql_type_to_marshal_type(&ck.data_type);
                    buffer.write_all(&encode_vuint(ck_marshal.len() as u64))?;
                    buffer.write_all(ck_marshal.as_bytes())?;
                }

                // Collect partition key and clustering key names for filtering
                let pk_names: std::collections::HashSet<&str> =
                    s.partition_keys.iter().map(|k| k.name.as_str()).collect();
                let ck_names: std::collections::HashSet<&str> =
                    s.clustering_keys.iter().map(|k| k.name.as_str()).collect();

                // staticColumns: filter for is_static && not PK/CK, sorted alphabetically
                let mut static_cols: Vec<_> = s
                    .columns
                    .iter()
                    .filter(|c| {
                        c.is_static
                            && !pk_names.contains(c.name.as_str())
                            && !ck_names.contains(c.name.as_str())
                    })
                    .collect();
                static_cols.sort_by(|a, b| a.name.cmp(&b.name));
                buffer.write_all(&encode_vuint(static_cols.len() as u64))?;
                for col in &static_cols {
                    // Column name: VUInt length + UTF-8 bytes
                    buffer.write_all(&encode_vuint(col.name.len() as u64))?;
                    buffer.write_all(col.name.as_bytes())?;
                    // Column type: VUInt length + marshal type bytes
                    let col_marshal = cql_type_to_marshal_type(&col.data_type);
                    buffer.write_all(&encode_vuint(col_marshal.len() as u64))?;
                    buffer.write_all(col_marshal.as_bytes())?;
                }

                // regularColumns: filter for !is_static && not PK/CK, sorted alphabetically
                // Cassandra's SerializationHeader stores columns in natural order (alphabetical)
                let mut regular_cols: Vec<_> = s
                    .columns
                    .iter()
                    .filter(|c| {
                        !c.is_static
                            && !pk_names.contains(c.name.as_str())
                            && !ck_names.contains(c.name.as_str())
                    })
                    .collect();
                regular_cols.sort_by(|a, b| a.name.cmp(&b.name));
                buffer.write_all(&encode_vuint(regular_cols.len() as u64))?;
                for col in &regular_cols {
                    buffer.write_all(&encode_vuint(col.name.len() as u64))?;
                    buffer.write_all(col.name.as_bytes())?;
                    let col_marshal = cql_type_to_marshal_type(&col.data_type);
                    buffer.write_all(&encode_vuint(col_marshal.len() as u64))?;
                    buffer.write_all(col_marshal.as_bytes())?;
                }
            }
            None => {
                // Minimal stub: BytesType key, no clustering, no columns
                let key_type = b"org.apache.cassandra.db.marshal.BytesType";
                buffer.write_all(&encode_vuint(key_type.len() as u64))?;
                buffer.write_all(key_type)?;

                // clusteringTypes: 0
                buffer.write_all(&encode_vuint(0))?;
                // staticColumns: 0
                buffer.write_all(&encode_vuint(0))?;
                // regularColumns: 0
                buffer.write_all(&encode_vuint(0))?;
            }
        }

        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_statistics_metadata_default() {
        let meta = StatisticsMetadata::new();
        assert_eq!(meta.partition_count, 0);
        assert_eq!(meta.row_count, 0);
    }

    #[test]
    fn test_statistics_metadata_update_timestamp() {
        let mut meta = StatisticsMetadata::new();
        meta.update_timestamp(1000000);
        meta.update_timestamp(2000000);
        meta.update_timestamp(500000);

        assert_eq!(meta.min_timestamp, 500000);
        assert_eq!(meta.max_timestamp, 2000000);
    }

    /// Issue #851 / Cassandra `d5bc7fb5`: a LIVE complex-deletion / liveness
    /// marker must not poison the timestamp aggregates. CQLite encodes
    /// `DeletionTime.LIVE` as `Long.MIN_VALUE`; Cassandra's `NO_DELETION` /
    /// `NO_TIMESTAMP` is `Long.MAX_VALUE`. Both must be ignored.
    #[test]
    fn test_update_timestamp_ignores_live_markers() {
        let mut meta = StatisticsMetadata::new();
        meta.update_timestamp(1_000_000);

        // LIVE marker (CQLite sentinel) must not pull min down to i64::MIN.
        meta.update_timestamp(i64::MIN);
        // NO_DELETION / NO_TIMESTAMP (Cassandra sentinel) must not push max up.
        meta.update_timestamp(i64::MAX);

        assert_eq!(
            meta.min_timestamp, 1_000_000,
            "LIVE marker must not poison min_timestamp"
        );
        assert_eq!(
            meta.max_timestamp, 1_000_000,
            "NO_DELETION marker must not poison max_timestamp"
        );
    }

    /// A LIVE complex-deletion marker (`localDeletionTime == Integer.MAX_VALUE`)
    /// is not a tombstone: it must not lower `min_local_deletion_time` nor inflate
    /// the tombstone drop-time histogram (issue #851, Cassandra `d5bc7fb5`).
    #[test]
    fn test_update_local_deletion_time_ignores_live_marker() {
        let mut meta = StatisticsMetadata::new();
        meta.update_local_deletion_time(1_500_000_000);

        // LIVE marker: must be skipped entirely.
        meta.update_local_deletion_time(i32::MAX);

        assert_eq!(
            meta.min_local_deletion_time, 1_500_000_000,
            "LIVE marker must not poison min_local_deletion_time"
        );
        assert_eq!(meta.max_local_deletion_time, 1_500_000_000);
        // Only the one real tombstone bin; the LIVE marker did not enter the histogram.
        assert_eq!(
            meta.tombstone_histogram.size(),
            1,
            "LIVE marker must not be counted as a tombstone in the histogram"
        );
    }

    /// With only LIVE markers, stats remain at sentinels and `finalize()`
    /// normalizes them to 0 (no tombstones recorded).
    #[test]
    fn test_only_live_markers_finalize_to_zero() {
        let mut meta = StatisticsMetadata::new();
        meta.update_timestamp(i64::MIN);
        meta.update_timestamp(i64::MAX);
        meta.update_local_deletion_time(i32::MAX);

        assert!(meta.tombstone_histogram.is_empty());

        meta.finalize();
        assert_eq!(meta.min_timestamp, 0);
        assert_eq!(meta.max_timestamp, 0);
        assert_eq!(meta.min_local_deletion_time, 0);
        assert_eq!(meta.max_local_deletion_time, 0);
    }

    #[test]
    fn test_statistics_metadata_update_ttl() {
        let mut meta = StatisticsMetadata::new();
        meta.update_ttl(3600);
        meta.update_ttl(86400);
        meta.update_ttl(1800);

        assert_eq!(meta.min_ttl, 1800);
        assert_eq!(meta.max_ttl, 86400);
    }

    #[test]
    fn test_statistics_metadata_finalize() {
        let mut meta = StatisticsMetadata::new();
        // Don't set any values
        meta.finalize();

        // Should normalize sentinel values to 0
        assert_eq!(meta.min_timestamp, 0);
        assert_eq!(meta.max_timestamp, 0);
        assert_eq!(meta.min_local_deletion_time, 0);
        assert_eq!(meta.max_local_deletion_time, 0);
        assert_eq!(meta.min_ttl, 0);
    }

    #[test]
    fn test_statistics_writer_basic() {
        let temp_dir = TempDir::new().unwrap();
        let stats_path = temp_dir.path().join("test-Statistics.db");

        let writer = StatisticsWriter::new(stats_path.clone());

        let mut meta = StatisticsMetadata::new();
        meta.update_timestamp(1000000);
        meta.update_timestamp(2000000);
        meta.min_local_deletion_time = 0;
        meta.max_local_deletion_time = 0;
        meta.min_ttl = 0;
        meta.max_ttl = 0;
        meta.partition_count = 10;
        meta.row_count = 100;

        let result = writer.write(&meta, None);
        assert!(result.is_ok(), "Write should succeed: {:?}", result);

        // Verify file was created
        assert!(stats_path.exists());

        // Verify file is not empty
        let file_size = std::fs::metadata(&stats_path).unwrap().len();
        assert!(file_size > 0, "Statistics.db should not be empty");

        // Read back and verify TOC structure
        let file_data = std::fs::read(&stats_path).unwrap();
        assert!(
            file_data.len() >= 44,
            "File should have at least 44 bytes (TOC)"
        );

        // Verify num_components = 4 (bytes 0-3)
        let num_components =
            u32::from_be_bytes([file_data[0], file_data[1], file_data[2], file_data[3]]);
        assert_eq!(num_components, 4, "Should have num_components=4");

        // Verify first checksum (bytes 4-7) matches CRC32(num_components)
        let checksum1 =
            u32::from_be_bytes([file_data[4], file_data[5], file_data[6], file_data[7]]);
        let expected_checksum1 = crc32fast::hash(&num_components.to_be_bytes());
        assert_eq!(
            checksum1, expected_checksum1,
            "First checksum should match CRC32(num_components)"
        );

        // Verify TOC entries exist (bytes 8-39)
        // Each entry is 8 bytes: 4 for type, 4 for offset
        assert!(file_data.len() >= 40, "Should have space for TOC entries");

        // Verify TOC checksum at byte 40
        assert!(file_data.len() >= 44, "Should have TOC checksum at byte 40");
    }

    #[test]
    fn test_build_validation_component() {
        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let result = writer.build_validation_component();
        assert!(result.is_ok());

        let bytes = result.unwrap();
        assert!(!bytes.is_empty());

        // Should contain partitioner class name (Java writeUTF format: u16 BE length + UTF-8)
        let partitioner = b"org.apache.cassandra.dht.Murmur3Partitioner";
        assert!(bytes.windows(partitioner.len()).any(|w| w == partitioner));

        // Should also contain bloom filter FP chance (f64 BE) = 0.01
        // Total length should be: 2 (length) + 43 (partitioner) + 8 (f64) = 53 bytes
        assert_eq!(bytes.len(), 53);
    }

    #[test]
    fn test_build_stats_component() {
        let writer = StatisticsWriter::new(PathBuf::from("test.db"));

        let mut meta = StatisticsMetadata::new();
        meta.min_timestamp = 1000000;
        meta.max_timestamp = 2000000;
        meta.min_local_deletion_time = 0;
        meta.max_local_deletion_time = 0;
        meta.min_ttl = 0;
        meta.max_ttl = 0;
        meta.partition_count = 100;
        meta.row_count = 100;
        meta.column_count = 200;

        let result = writer.build_stats_component(&meta);
        assert!(result.is_ok());

        let data = result.unwrap();
        assert!(!data.is_empty());

        // STATS component now has a complex binary format (nb version)
        // It should contain:
        // - 2x EstimatedHistogram (2 buckets each = 36 bytes each)
        // - CommitLogPosition upper bound (12 bytes)
        // - min/max timestamps (16 bytes)
        // - min/max deletion times (8 bytes)
        // - min/max TTL (8 bytes)
        // - compression ratio (8 bytes)
        // - TombstoneHistogram (8 bytes for empty)
        // - sstableLevel (4 bytes)
        // - repairedAt (8 bytes)
        // - min/max clustering count (8 bytes)
        // - hasLegacyCounterShards (1 byte)
        // - totalColumnsSet (8 bytes)
        // - totalRows (8 bytes)
        // - CommitLogPosition lower bound (12 bytes)
        // - commitLogIntervals empty set (4 bytes)
        // - pendingRepair (1 byte)
        // - isTransient (1 byte)
        // - originatingHostId (1 byte)
        // Total: 36+36+12+16+8+8+8+8+4+8+8+1+8+8+12+4+1+1+1 = 188 bytes
        assert_eq!(data.len(), 188);

        // Verify the row count is present (at offset 36+36+12+16+8+8+8+8+4+8+8+1+8 = 161)
        let row_count_offset = 161;
        let row_count_bytes = &data[row_count_offset..row_count_offset + 8];
        let row_count = u64::from_be_bytes(row_count_bytes.try_into().unwrap());
        assert_eq!(row_count, 100);
    }

    /// The `da` STATS body must serialise `hasPartitionLevelDeletions` as a
    /// single `writeBoolean` byte: `0x01` when the SSTable contains a
    /// partition-level deletion, `0x00` otherwise. Authority: cassandra-5.0.0
    /// `StatsMetadata.StatsMetadataSerializer.serialize` line 495
    /// (`out.writeBoolean(component.hasPartitionLevelDeletions)`), gated by
    /// `version.hasPartitionLevelDeletionsPresenceMarker()` which is `true` for
    /// `BtiFormat.BtiVersion`.
    #[test]
    fn test_da_stats_has_partition_level_deletions_marker_byte() {
        let writer = StatisticsWriter::new_bti(PathBuf::from("da-1-bti-Statistics.db"));

        // Identical metadata except for the partition-level-deletion flag, so
        // the only differing byte is the hasPartitionLevelDeletions marker.
        let mut base = StatisticsMetadata::new();
        base.min_timestamp = 1_000_000;
        base.max_timestamp = 2_000_000;
        base.min_local_deletion_time = 0;
        base.max_local_deletion_time = 0;
        base.partition_count = 1;
        base.row_count = 1;
        base.column_count = 1;
        base.finalize();

        let mut with_del = base.clone();
        with_del.mark_partition_level_deletion();
        assert!(with_del.has_partition_level_deletions);
        assert!(!base.has_partition_level_deletions);

        let no_del_bytes = writer.build_stats_component_da(&base, None).unwrap();
        let del_bytes = writer.build_stats_component_da(&with_del, None).unwrap();

        // Same length: only the one marker byte changes value.
        assert_eq!(
            no_del_bytes.len(),
            del_bytes.len(),
            "the flag must not change the serialized length"
        );

        // Exactly one byte differs, and it flips 0x00 <-> 0x01.
        let diffs: Vec<usize> = no_del_bytes
            .iter()
            .zip(del_bytes.iter())
            .enumerate()
            .filter(|(_, (a, b))| a != b)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            diffs.len(),
            1,
            "only the hasPartitionLevelDeletions marker byte should differ"
        );
        let marker = diffs[0];
        assert_eq!(
            no_del_bytes[marker], 0x00,
            "no partition deletion => marker byte 0x00"
        );
        assert_eq!(
            del_bytes[marker], 0x01,
            "partition deletion present => marker byte 0x01"
        );
    }

    #[test]
    fn test_checksums_format() {
        let temp_dir = TempDir::new().unwrap();
        let stats_path = temp_dir.path().join("test-Statistics.db");

        let writer = StatisticsWriter::new(stats_path.clone());

        let mut meta = StatisticsMetadata::new();
        meta.min_timestamp = 1000000;
        meta.partition_count = 10;

        writer.write(&meta, None).unwrap();

        // Read file and verify checksum structure
        let file_data = std::fs::read(&stats_path).unwrap();

        // Parse and verify count checksum
        let num_components =
            u32::from_be_bytes([file_data[0], file_data[1], file_data[2], file_data[3]]);
        let checksum1 =
            u32::from_be_bytes([file_data[4], file_data[5], file_data[6], file_data[7]]);

        let mut crc = crc32fast::Hasher::new();
        crc.update(&num_components.to_be_bytes());
        let expected_checksum1 = crc.finalize();

        assert_eq!(checksum1, expected_checksum1, "Count checksum should match");

        // Parse TOC entries and verify cumulative checksum
        let mut crc = crc32fast::Hasher::new();
        crc.update(&num_components.to_be_bytes());

        for i in 0..num_components {
            let offset = 8 + (i as usize * 8);
            let comp_type = u32::from_be_bytes([
                file_data[offset],
                file_data[offset + 1],
                file_data[offset + 2],
                file_data[offset + 3],
            ]);
            let comp_offset = u32::from_be_bytes([
                file_data[offset + 4],
                file_data[offset + 5],
                file_data[offset + 6],
                file_data[offset + 7],
            ]);

            crc.update(&comp_type.to_be_bytes());
            crc.update(&comp_offset.to_be_bytes());
        }

        let toc_checksum =
            u32::from_be_bytes([file_data[40], file_data[41], file_data[42], file_data[43]]);
        let expected_toc_checksum = crc.finalize();

        assert_eq!(
            toc_checksum, expected_toc_checksum,
            "TOC checksum should match cumulative CRC32"
        );
    }

    #[test]
    fn test_component_checksums() {
        let temp_dir = TempDir::new().unwrap();
        let stats_path = temp_dir.path().join("test-Statistics.db");

        let writer = StatisticsWriter::new(stats_path.clone());

        let mut meta = StatisticsMetadata::new();
        meta.min_timestamp = 1000000;
        meta.partition_count = 100;

        writer.write(&meta, None).unwrap();

        // Read file and verify per-component checksums
        let file_data = std::fs::read(&stats_path).unwrap();

        // Parse TOC to get component offsets
        let num_components =
            u32::from_be_bytes([file_data[0], file_data[1], file_data[2], file_data[3]]);
        assert_eq!(num_components, 4);

        let mut component_offsets = Vec::new();
        for i in 0..num_components {
            let offset = 8 + (i as usize * 8) + 4; // +4 to skip type, get offset
            let comp_offset = u32::from_be_bytes([
                file_data[offset],
                file_data[offset + 1],
                file_data[offset + 2],
                file_data[offset + 3],
            ]);
            component_offsets.push(comp_offset as usize);
        }

        // Verify each component's checksum
        for i in 0..num_components as usize {
            let comp_start = component_offsets[i];

            // Calculate component length
            let comp_end = if i < component_offsets.len() - 1 {
                component_offsets[i + 1]
            } else {
                file_data.len()
            };

            // Component data ends 4 bytes before next component (for checksum)
            let comp_length = comp_end - comp_start - 4;
            let component_data = &file_data[comp_start..comp_start + comp_length];

            // Read stored checksum
            let stored_checksum = u32::from_be_bytes([
                file_data[comp_start + comp_length],
                file_data[comp_start + comp_length + 1],
                file_data[comp_start + comp_length + 2],
                file_data[comp_start + comp_length + 3],
            ]);

            // Compute expected checksum
            let computed_checksum = crc32fast::hash(component_data);

            assert_eq!(
                stored_checksum, computed_checksum,
                "Component {} checksum mismatch",
                i
            );
        }
    }

    #[test]
    fn test_component_binary_formats() {
        let temp_dir = TempDir::new().unwrap();
        let stats_path = temp_dir.path().join("test-Statistics.db");

        let writer = StatisticsWriter::new(stats_path.clone());

        let mut meta = StatisticsMetadata::new();
        // Use realistic values at or above epoch baselines to avoid wrapping
        // in EncodingStats delta encoding (TIMESTAMP_EPOCH = 1442880000000000,
        // DELETION_TIME_EPOCH = 1442880000, TTL_EPOCH = 0).
        meta.min_timestamp = TIMESTAMP_EPOCH;
        meta.max_timestamp = TIMESTAMP_EPOCH + 1000000;
        meta.min_local_deletion_time = DELETION_TIME_EPOCH;
        meta.max_local_deletion_time = DELETION_TIME_EPOCH + 100;
        meta.min_ttl = 0;
        meta.max_ttl = 200;
        meta.partition_count = 50;
        meta.row_count = 150;
        meta.column_count = 300;

        writer.write(&meta, None).unwrap();

        // Read and parse the file
        let file_data = std::fs::read(&stats_path).unwrap();

        // Verify TOC structure
        let num_components =
            u32::from_be_bytes([file_data[0], file_data[1], file_data[2], file_data[3]]);
        assert_eq!(num_components, 4, "Should have 4 components");

        // Read component offsets
        let validation_offset =
            u32::from_be_bytes([file_data[12], file_data[13], file_data[14], file_data[15]])
                as usize;
        let compaction_offset =
            u32::from_be_bytes([file_data[20], file_data[21], file_data[22], file_data[23]])
                as usize;
        let stats_offset =
            u32::from_be_bytes([file_data[28], file_data[29], file_data[30], file_data[31]])
                as usize;
        let header_offset =
            u32::from_be_bytes([file_data[36], file_data[37], file_data[38], file_data[39]])
                as usize;

        // Verify VALIDATION component format
        // First 2 bytes should be u16 BE length of partitioner string
        let partitioner_len = u16::from_be_bytes([
            file_data[validation_offset],
            file_data[validation_offset + 1],
        ]);
        assert_eq!(
            partitioner_len, 43,
            "Partitioner string length should be 43"
        );

        // Verify COMPACTION component format
        // First 4 bytes should be i32 BE length of HLL data
        let hll_len = i32::from_be_bytes([
            file_data[compaction_offset],
            file_data[compaction_offset + 1],
            file_data[compaction_offset + 2],
            file_data[compaction_offset + 3],
        ]);
        assert_eq!(hll_len, 15, "HLL data length should be 15 bytes");

        // Verify HLL version marker (next 4 bytes should be -2 = 0xFFFFFFFE)
        let hll_version = i32::from_be_bytes([
            file_data[compaction_offset + 4],
            file_data[compaction_offset + 5],
            file_data[compaction_offset + 6],
            file_data[compaction_offset + 7],
        ]);
        assert_eq!(hll_version, -2, "HLL version should be -2");

        // Verify STATS component has correct total size (188 bytes + 4 byte checksum)
        let stats_end = header_offset;
        let stats_size = stats_end - stats_offset - 4; // -4 for checksum
        assert_eq!(stats_size, 188, "STATS component should be 188 bytes");

        // Verify min_timestamp in STATS component (at offset: 2*36 + 12 = 84 from stats_offset)
        let ts_offset = stats_offset + 84;
        let min_ts = i64::from_be_bytes([
            file_data[ts_offset],
            file_data[ts_offset + 1],
            file_data[ts_offset + 2],
            file_data[ts_offset + 3],
            file_data[ts_offset + 4],
            file_data[ts_offset + 5],
            file_data[ts_offset + 6],
            file_data[ts_offset + 7],
        ]);
        assert_eq!(min_ts, TIMESTAMP_EPOCH, "Min timestamp should be preserved");

        // Verify SERIALIZATION_HEADER component
        // Should start with 3 unsigned VInts for EncodingStats deltas.
        // All metadata values == their epoch baselines, so all deltas are 0.
        // encode_vuint(0) = [0x00].
        assert_eq!(
            file_data[header_offset], 0x00,
            "EncodingStats minTimestamp delta should be 0"
        );
        assert_eq!(
            file_data[header_offset + 1],
            0x00,
            "EncodingStats minLocalDeletionTime delta should be 0"
        );
        assert_eq!(
            file_data[header_offset + 2],
            0x00,
            "EncodingStats minTTL delta should be 0"
        );
    }

    #[test]
    fn test_cql_type_to_marshal_type() {
        assert_eq!(
            cql_type_to_marshal_type("text"),
            "org.apache.cassandra.db.marshal.UTF8Type"
        );
        assert_eq!(
            cql_type_to_marshal_type("int"),
            "org.apache.cassandra.db.marshal.Int32Type"
        );
        assert_eq!(
            cql_type_to_marshal_type("bigint"),
            "org.apache.cassandra.db.marshal.LongType"
        );
        assert_eq!(
            cql_type_to_marshal_type("uuid"),
            "org.apache.cassandra.db.marshal.UUIDType"
        );
        assert_eq!(
            cql_type_to_marshal_type("blob"),
            "org.apache.cassandra.db.marshal.BytesType"
        );
        assert_eq!(
            cql_type_to_marshal_type("timestamp"),
            "org.apache.cassandra.db.marshal.TimestampType"
        );
        assert_eq!(
            cql_type_to_marshal_type("boolean"),
            "org.apache.cassandra.db.marshal.BooleanType"
        );
        assert_eq!(
            cql_type_to_marshal_type("varint"),
            "org.apache.cassandra.db.marshal.IntegerType"
        );
        // Unknown type falls back to BytesType
        assert_eq!(
            cql_type_to_marshal_type("unknown_type"),
            "org.apache.cassandra.db.marshal.BytesType"
        );

        // Collection types
        assert_eq!(
            cql_type_to_marshal_type("list<int>"),
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)"
        );
        assert_eq!(
            cql_type_to_marshal_type("set<text>"),
            "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)"
        );
        assert_eq!(
            cql_type_to_marshal_type("map<text, int>"),
            "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)"
        );

        // Frozen and nested
        assert_eq!(
            cql_type_to_marshal_type("frozen<list<int>>"),
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))"
        );

        // Tuple
        assert_eq!(
            cql_type_to_marshal_type("tuple<int, text>"),
            "org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.UTF8Type)"
        );
    }

    #[test]
    fn test_serialization_header_with_schema() {
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "age".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
        };

        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let meta = StatisticsMetadata::new();
        let result = writer.build_serialization_header_component(Some(&schema), &meta);
        assert!(result.is_ok());

        let bytes = result.unwrap();

        // Verify the header contains the UUIDType key type
        let header_str = String::from_utf8_lossy(&bytes);
        assert!(
            header_str.contains("UUIDType"),
            "Header should contain UUIDType for uuid partition key"
        );

        // Verify column names are present
        assert!(
            header_str.contains("name"),
            "Header should contain column 'name'"
        );
        assert!(
            header_str.contains("age"),
            "Header should contain column 'age'"
        );

        // Verify column types are present
        assert!(
            header_str.contains("UTF8Type"),
            "Header should contain UTF8Type for text column"
        );
        assert!(
            header_str.contains("Int32Type"),
            "Header should contain Int32Type for int column"
        );
    }

    #[test]
    fn test_serialization_header_composite_partition_key() {
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "composite_table".to_string(),
            partition_keys: vec![
                KeyColumn {
                    name: "tenant".to_string(),
                    data_type: "text".to_string(),
                    position: 0,
                },
                KeyColumn {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    position: 1,
                },
            ],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "tenant".to_string(),
                    data_type: "text".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "value".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
        };

        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let meta = StatisticsMetadata::new();
        let bytes = writer
            .build_serialization_header_component(Some(&schema), &meta)
            .unwrap();

        let header_str = String::from_utf8_lossy(&bytes);
        assert!(
            header_str.contains("CompositeType("),
            "Composite PK should produce CompositeType wrapper"
        );
        assert!(
            header_str.contains("UTF8Type"),
            "CompositeType should contain UTF8Type for text PK"
        );
        assert!(
            header_str.contains("UUIDType"),
            "CompositeType should contain UUIDType for uuid PK"
        );
    }

    // -----------------------------------------------------------------------
    // TombstoneHistogram unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_tombstone_histogram_empty() {
        let h = TombstoneHistogram::new();
        assert!(h.is_empty());
        assert_eq!(h.size(), 0);
    }

    #[test]
    fn test_tombstone_histogram_single_entry() {
        let mut h = TombstoneHistogram::new();
        h.update(1_700_000_000);
        assert!(!h.is_empty());
        assert_eq!(h.size(), 1);
        let entries: Vec<_> = h.entries().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 1_700_000_000.0f64);
        assert_eq!(entries[0].1, 1);
    }

    #[test]
    fn test_tombstone_histogram_multiple_entries() {
        let mut h = TombstoneHistogram::new();
        // Three distinct deletion times
        h.update(1_000);
        h.update(2_000);
        h.update(1_000); // duplicate — should increment count
        assert_eq!(h.size(), 2);
        let entries: Vec<_> = h.entries().collect();
        // Entries are sorted by point value ascending
        assert_eq!(entries[0].0, 1_000.0f64);
        assert_eq!(entries[0].1, 2); // count = 2
        assert_eq!(entries[1].0, 2_000.0f64);
        assert_eq!(entries[1].1, 1);
    }

    #[test]
    fn test_tombstone_histogram_bin_merge_at_capacity() {
        let mut h = TombstoneHistogram::new();
        // Insert 101 distinct deletion times — should trigger a merge so bins <= 100
        for i in 0..=100i32 {
            h.update(1_700_000_000 + i);
        }
        assert!(
            h.size() <= TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE,
            "bins should not exceed MAX_BIN_SIZE after merge: got {}",
            h.size()
        );
        assert!(!h.is_empty());
    }

    // -----------------------------------------------------------------------
    // TombstoneHistogram serialisation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_tombstone_histogram_empty() {
        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let h = TombstoneHistogram::new();
        let mut buf = Vec::new();
        writer.write_tombstone_histogram(&mut buf, &h).unwrap();

        // Empty: 4 bytes maxBinSize=0 + 4 bytes size=0 = 8 bytes total
        assert_eq!(buf.len(), 8);
        let max_bin_size = i32::from_be_bytes(buf[0..4].try_into().unwrap());
        let size = i32::from_be_bytes(buf[4..8].try_into().unwrap());
        assert_eq!(max_bin_size, 0, "empty histogram maxBinSize should be 0");
        assert_eq!(size, 0, "empty histogram size should be 0");
    }

    #[test]
    fn test_write_tombstone_histogram_nonempty() {
        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let mut h = TombstoneHistogram::new();
        h.update(1_700_000_000);
        h.update(1_700_000_100);
        let mut buf = Vec::new();
        writer.write_tombstone_histogram(&mut buf, &h).unwrap();

        // Non-empty with 2 bins:
        //   4 bytes maxBinSize=100
        //   4 bytes size=2
        //   2 × (8 f64 + 8 i64) = 32 bytes
        // Total = 40 bytes
        assert_eq!(buf.len(), 40, "2-bin histogram should be 40 bytes");

        let max_bin_size = i32::from_be_bytes(buf[0..4].try_into().unwrap());
        let size = i32::from_be_bytes(buf[4..8].try_into().unwrap());
        assert_eq!(
            max_bin_size, 100,
            "non-empty histogram maxBinSize should be 100"
        );
        assert_eq!(size, 2, "histogram size should be 2");

        // Verify first bin point (should be 1_700_000_000.0)
        let point0 = f64::from_be_bytes(buf[8..16].try_into().unwrap());
        assert_eq!(point0, 1_700_000_000.0f64);
        let value0 = i64::from_be_bytes(buf[16..24].try_into().unwrap());
        assert_eq!(value0, 1);

        // Verify second bin point (should be 1_700_000_100.0)
        let point1 = f64::from_be_bytes(buf[24..32].try_into().unwrap());
        assert_eq!(point1, 1_700_000_100.0f64);
        let value1 = i64::from_be_bytes(buf[32..40].try_into().unwrap());
        assert_eq!(value1, 1);
    }

    /// Verify that Statistics.db written for a table WITH tombstones produces a
    /// non-empty `estimatedTombstoneDropTime` histogram in the STATS component.
    ///
    /// This is the primary acceptance-criterion test for issue #730.
    #[test]
    fn test_statistics_db_tombstone_histogram_nonempty() {
        let temp_dir = TempDir::new().unwrap();
        let stats_path = temp_dir.path().join("tombstone-Statistics.db");
        let writer = StatisticsWriter::new(stats_path.clone());

        // Simulate two tombstone local-deletion-times
        let ldt1 = 1_700_000_000i32;
        let ldt2 = 1_700_100_000i32;

        let mut meta = StatisticsMetadata::new();
        meta.update_timestamp(1_600_000_000_000_000); // some live write
        meta.update_local_deletion_time(ldt1);
        meta.update_local_deletion_time(ldt2);
        meta.partition_count = 1;
        meta.row_count = 2;
        meta.column_count = 4;

        writer.write(&meta, None).expect("write should succeed");

        // ---- Parse the resulting Statistics.db and find the histogram ----
        let file_data = std::fs::read(&stats_path).expect("file should exist");

        // The STATS component offset is stored in the TOC at bytes 28–31
        // (3rd entry, 4-byte type + 4-byte offset = 8 bytes each, starting at byte 8;
        //  entries: [8..16] VALIDATION, [16..24] COMPACTION, [24..32] STATS, [32..40] HEADER)
        let stats_offset =
            u32::from_be_bytes([file_data[28], file_data[29], file_data[30], file_data[31]])
                as usize;

        // Within the STATS component, the histogram field starts after:
        //   2 × EstimatedHistogram  = 2 × 36 = 72 bytes
        //   CommitLogPosition upper = 12 bytes
        //   minTimestamp (i64)      =  8 bytes
        //   maxTimestamp (i64)      =  8 bytes
        //   minLocalDeletionTime    =  4 bytes
        //   maxLocalDeletionTime    =  4 bytes
        //   minTTL                  =  4 bytes
        //   maxTTL                  =  4 bytes
        //   compressionRatio (f64)  =  8 bytes
        // = 72 + 12 + 8 + 8 + 4 + 4 + 4 + 4 + 8 = 124 bytes
        let histogram_offset = stats_offset + 124;

        // Read maxBinSize and size
        let max_bin_size = i32::from_be_bytes(
            file_data[histogram_offset..histogram_offset + 4]
                .try_into()
                .expect("histogram maxBinSize bytes"),
        );
        let histo_size = i32::from_be_bytes(
            file_data[histogram_offset + 4..histogram_offset + 8]
                .try_into()
                .expect("histogram size bytes"),
        );

        assert_eq!(
            max_bin_size,
            TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE,
            "estimatedTombstoneDropTime maxBinSize should be {} for a non-empty histogram (issue #730)",
            TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE
        );
        assert_eq!(
            histo_size, 2,
            "estimatedTombstoneDropTime should have 2 bins for 2 distinct deletion times (issue #730)"
        );

        // Verify the first bin's point matches ldt1
        let point0 = f64::from_be_bytes(
            file_data[histogram_offset + 8..histogram_offset + 16]
                .try_into()
                .expect("bin0 point bytes"),
        );
        assert_eq!(
            point0, ldt1 as f64,
            "first histogram bin point should match ldt1"
        );

        // Verify the second bin's point matches ldt2
        let point1 = f64::from_be_bytes(
            file_data[histogram_offset + 24..histogram_offset + 32]
                .try_into()
                .expect("bin1 point bytes"),
        );
        assert_eq!(
            point1, ldt2 as f64,
            "second histogram bin point should match ldt2"
        );

        // Verify both bins have count = 1
        let value0 = i64::from_be_bytes(
            file_data[histogram_offset + 16..histogram_offset + 24]
                .try_into()
                .expect("bin0 value bytes"),
        );
        let value1 = i64::from_be_bytes(
            file_data[histogram_offset + 32..histogram_offset + 40]
                .try_into()
                .expect("bin1 value bytes"),
        );
        assert_eq!(value0, 1, "first bin count should be 1");
        assert_eq!(value1, 1, "second bin count should be 1");
    }

    /// Verify that `StatisticsMetadata::update_local_deletion_time` feeds the histogram.
    #[test]
    fn test_metadata_update_local_deletion_time_populates_histogram() {
        let mut meta = StatisticsMetadata::new();
        assert!(meta.tombstone_histogram.is_empty());

        meta.update_local_deletion_time(1_700_000_000);
        meta.update_local_deletion_time(1_700_100_000);
        meta.update_local_deletion_time(1_700_000_000); // duplicate

        assert!(!meta.tombstone_histogram.is_empty());
        assert_eq!(
            meta.tombstone_histogram.size(),
            2,
            "two distinct ldts → 2 bins"
        );

        // The bin for 1_700_000_000 should have count 2
        let entries: Vec<_> = meta.tombstone_histogram.entries().collect();
        let (pt0, v0) = entries[0];
        assert_eq!(pt0, 1_700_000_000.0f64);
        assert_eq!(v0, 2);
    }

    /// Parse a Cassandra SSTable SERIALIZATION_HEADER column-set the way Cassandra's
    /// `SerializationHeader.Serializer.readColumnsWithType` does (cassandra-5.0.0
    /// `SerializationHeader.java` lines 510-520):
    ///
    /// ```text
    /// unsigned-vint  count
    /// repeat count times:
    ///   vint-length-prefixed UTF-8  column name
    ///   vint-length-prefixed UTF-8  marshal type
    /// ```
    ///
    /// Returns the parsed `(name, marshal_type)` pairs and the slice remaining after
    /// the column set, so chained sets (static then regular) can be parsed.
    fn parse_columns_with_types(input: &[u8]) -> (Vec<(String, String)>, &[u8]) {
        use crate::parser::vint::parse_vuint;

        let (mut rest, count) = parse_vuint(input).expect("column count vint");
        let mut cols = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let (after_name_len, name_len) = parse_vuint(rest).expect("name length vint");
            let name =
                String::from_utf8(after_name_len[..name_len as usize].to_vec()).expect("name utf8");
            let after_name = &after_name_len[name_len as usize..];

            let (after_type_len, type_len) = parse_vuint(after_name).expect("type length vint");
            let marshal =
                String::from_utf8(after_type_len[..type_len as usize].to_vec()).expect("type utf8");
            rest = &after_type_len[type_len as usize..];

            cols.push((name, marshal));
        }
        (cols, rest)
    }

    /// Build a schema with `n` regular columns named `c00..c{n-1}` plus a uuid PK.
    fn wide_schema(n: usize) -> crate::schema::TableSchema {
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let mut columns = vec![Column {
            name: "id".to_string(),
            data_type: "uuid".to_string(),
            nullable: false,
            default: None,
            is_static: false,
        }];
        for i in 0..n {
            columns.push(Column {
                name: format!("c{i:03}"),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            });
        }

        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "wide_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns,
            comments: HashMap::new(),
        }
    }

    /// Regression test for issue #763: a table with more than 64 regular columns
    /// must produce a SERIALIZATION_HEADER that encodes every column.
    ///
    /// Cassandra's on-disk header uses `writeColumnsWithTypes` (SerializationHeader.java
    /// lines 489-497): an unsigned-VInt count followed by `count` (name, type) pairs.
    /// There is NO 64-bit bitmap on this path — the bitmap (`Columns.serializeSubset`,
    /// Columns.java lines 503-531) is only used for per-row column subsets against a
    /// pre-shared superset, never for the SSTable header. So a 70-column table is a
    /// fully supported, lossless encoding.
    #[test]
    fn test_serialization_header_70_columns_roundtrip() {
        let schema = wide_schema(70);
        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let meta = StatisticsMetadata::new();
        let bytes = writer
            .build_serialization_header_component(Some(&schema), &meta)
            .expect("build header for 70-column schema");

        // Skip the 3 EncodingStats VInts, the key type, and the clustering list to
        // reach the static/regular column sets.
        use crate::parser::vint::parse_vuint;
        let (rest, _min_ts) = parse_vuint(&bytes).expect("encoding stats minTimestamp");
        let (rest, _min_ldt) = parse_vuint(rest).expect("encoding stats minLocalDeletionTime");
        let (rest, _min_ttl) = parse_vuint(rest).expect("encoding stats minTTL");

        // keyType: vint-length-prefixed UTF-8
        let (rest, key_len) = parse_vuint(rest).expect("key type length");
        let key_type = std::str::from_utf8(&rest[..key_len as usize]).expect("key type utf8");
        assert_eq!(key_type, "org.apache.cassandra.db.marshal.UUIDType");
        let rest = &rest[key_len as usize..];

        // clusteringTypes: vint count (0 here)
        let (rest, ck_count) = parse_vuint(rest).expect("clustering count");
        assert_eq!(ck_count, 0, "no clustering columns");

        // staticColumns then regularColumns
        let (statics, rest) = parse_columns_with_types(rest);
        assert_eq!(statics.len(), 0, "no static columns");

        let (regulars, rest) = parse_columns_with_types(rest);
        assert!(rest.is_empty(), "header fully consumed, no trailing bytes");

        // All 70 regular columns must be present (the PK `id` is excluded).
        assert_eq!(
            regulars.len(),
            70,
            "all 70 regular columns must be encoded, got {}",
            regulars.len()
        );

        // Columns are emitted in alphabetical order; verify a sample round-trips.
        assert_eq!(regulars[0].0, "c000");
        assert_eq!(regulars[0].1, "org.apache.cassandra.db.marshal.Int32Type");
        assert_eq!(regulars[69].0, "c069");

        // Every column name and type must be intact (lossless).
        let mut names: Vec<String> = regulars.iter().map(|(n, _)| n.clone()).collect();
        names.sort();
        for (i, name) in names.iter().enumerate().take(70) {
            assert_eq!(*name, format!("c{i:03}"));
        }
    }

    /// Verify the column-count field is encoded as a true unsigned VInt (not a single
    /// byte). For 200 columns the count 200 (0xC8) requires a 2-byte VInt, which is
    /// where a naive single-byte writer would silently corrupt the header.
    #[test]
    fn test_serialization_header_200_columns_count_is_vint() {
        use crate::parser::vint::{encode_vuint, parse_vuint};

        let schema = wide_schema(200);
        let writer = StatisticsWriter::new(PathBuf::from("test.db"));
        let meta = StatisticsMetadata::new();
        let bytes = writer
            .build_serialization_header_component(Some(&schema), &meta)
            .expect("build header for 200-column schema");

        let (rest, _) = parse_vuint(&bytes).expect("minTimestamp");
        let (rest, _) = parse_vuint(rest).expect("minLocalDeletionTime");
        let (rest, _) = parse_vuint(rest).expect("minTTL");
        let (rest, key_len) = parse_vuint(rest).expect("key type length");
        let rest = &rest[key_len as usize..];
        let (rest, _ck) = parse_vuint(rest).expect("clustering count");
        let (statics, rest) = parse_columns_with_types(rest);
        assert_eq!(statics.len(), 0);

        // The regular-column count must be the 2-byte VInt encoding of 200.
        let expected_count_bytes = encode_vuint(200);
        assert_eq!(
            expected_count_bytes.len(),
            2,
            "200 must require a 2-byte VInt (sanity)"
        );
        assert_eq!(
            &rest[..expected_count_bytes.len()],
            expected_count_bytes.as_slice(),
            "regular-column count must be a multi-byte unsigned VInt"
        );

        let (regulars, tail) = parse_columns_with_types(rest);
        assert!(tail.is_empty());
        assert_eq!(regulars.len(), 200, "all 200 columns must be encoded");
    }

    /// Regression: the EncodingStats `minLocalDeletionTime` delta must be written
    /// as the SIGN-EXTENDED `int` delta `minLocalDeletionTime - DELETION_TIME_EPOCH`,
    /// because Cassandra recovers it with `readUnsignedVInt32()` →
    /// `VIntCoding.checkedCast`, which REJECTS any decoded value that does not
    /// round-trip through a signed 32-bit `int` (`(int)value != value`). A bare
    /// `u32` truncation of a small LDT (e.g. delta `2852087298` for LDT `2`) is out
    /// of range for `checkedCast` and makes `cassandra:5.0` `sstabledump` reject the
    /// SSTable (verified live; see tests/issue_911_bti_partition_deletion_stats.rs).
    ///
    /// A far-future LDT in `[2^31, 2^32)` is stored as a negative i32 bit pattern,
    /// which IS the signed int the reader expects, so the same sign-extending path
    /// handles it. This pins both: the small-LDT delta is a sign-extended 64-bit
    /// VInt, and the decoded value round-trips through a signed i32.
    #[test]
    fn test_serialization_header_ldt_baseline_sign_extends_for_checked_cast() {
        use crate::parser::vint::parse_vuint;

        // Helper: decode the minLocalDeletionTime EncodingStats delta for a given
        // baseline and assert it round-trips through Cassandra's checkedCast (i.e.
        // the decoded u64, reinterpreted as i64, fits a signed i32).
        let decode_delta = |min_ldt: i32| -> u64 {
            let mut meta = StatisticsMetadata::new();
            meta.min_local_deletion_time = min_ldt;
            let writer = StatisticsWriter::new(PathBuf::from("test.db"));
            let bytes = writer
                .build_serialization_header_component(None, &meta)
                .expect("build header");
            let (rest, _min_ts_delta) = parse_vuint(&bytes).expect("minTimestamp delta");
            let (_rest, min_ldt_delta) = parse_vuint(rest).expect("minLocalDeletionTime delta");
            min_ldt_delta
        };

        // checkedCast accepts `value` iff `(int)value == value` — i.e. the decoded
        // u64, reinterpreted as i64, equals its own truncation to i32.
        let passes_checked_cast = |delta: u64| -> bool {
            let v = delta as i64;
            (v as i32) as i64 == v
        };

        // Small LDT (e.g. 2): the regression case. The delta is the negative int
        // `2 - DELETION_TIME_EPOCH`, sign-extended.
        let small = decode_delta(2);
        let expected_small = (2i32.wrapping_sub(DELETION_TIME_EPOCH) as i64) as u64;
        assert_eq!(small, expected_small);
        assert!(
            passes_checked_cast(small),
            "small-LDT delta must round-trip through a signed i32 (checkedCast), got {small:#x}"
        );

        // Far-future LDT in [2^31, 2^32): a negative i32 bit pattern. Same path.
        let far_future = ((1u32 << 31) + 5) as i32;
        assert!(far_future < 0, "sanity: far-future LDT is a negative i32");
        let far = decode_delta(far_future);
        let expected_far = (far_future.wrapping_sub(DELETION_TIME_EPOCH) as i64) as u64;
        assert_eq!(far, expected_far);
        assert!(
            passes_checked_cast(far),
            "far-future LDT delta must round-trip through a signed i32 (checkedCast), got {far:#x}"
        );
    }
}
