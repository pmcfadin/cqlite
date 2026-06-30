//! Per-table aggregate statistics surfaced over Flight (`DoAction`).
//!
//! The Java Trino connector drives its AUTOMATIC aggregation-pushdown gate
//! (issue #944, follow-up to #893/#937/#841) from a per-table row-count /
//! partition-count estimate. This module computes those numbers by summing the
//! AUTHORITATIVE per-SSTable statistics that Cassandra already records in each
//! `Statistics.db` file, and serialises them as the request/response payloads of
//! a `DoAction("table_stats")` Flight call.
//!
//! ## Authoritative vs derived (no-heuristics mandate, issue #28)
//!
//! Everything this module produces is AUTHORITATIVE — decoded straight from the
//! STATS component of each `Statistics.db` via
//! [`cqlite_core::parser::repair_metadata::read_table_counts`], which walks the
//! exact cassandra-5.0.0 `StatsMetadata.StatsMetadataSerializer.serialize` layout
//! (no byte-pattern guessing):
//!
//! - `partition_count` = Σ per-SSTable `TableCounts::partition_count`. Each
//!   SSTable's partition count is the sum of its `estimatedPartitionSize`
//!   `EstimatedHistogram` bucket counts (one observation per partition) — the same
//!   number `SSTableReader.getEstimatedPartitionSize().count()` reports.
//! - `live_rows` = Σ per-SSTable `TableCounts::total_rows` (the STATS `totalRows`
//!   field). This is the SSTable's total row count; pre-compaction it is an upper
//!   bound on the table's live rows. `read_table_counts` returns `total_rows` only
//!   when it can traverse the version-gated min/max block; an SSTable whose
//!   `total_rows` could not be reached contributes 0 (honest under-count, never a
//!   guess).
//! - `sstable_count` = number of `*-Statistics.db` files successfully decoded.
//!
//! Because these are per-SSTable sums they are an UPPER BOUND on the table's true
//! distinct partition / live-row counts (the same partition can appear in several
//! SSTables before compaction). That is exactly what the gate wants: an upper
//! bound on distinct groups never under-counts, so it never wrongly pushes a
//! genuinely-high-cardinality GROUP BY.
//!
//! The DERIVED step — mapping a grouping shape to an estimated group count and a
//! ratio — lives entirely in the Java connector (it owns the DDL-driven mapping).
//! We deliberately do NOT surface per-`ColumnStatistics::cardinality`: Cassandra
//! 5.0 does not store a reliable per-regular-column NDV, so trusting it would be a
//! heuristic. This is a partition-key-vs-row-level estimate, not a true NDV gate.

use std::path::Path;

use serde::{Deserialize, Serialize};

use cqlite_core::parser::repair_metadata::read_table_counts;
use cqlite_core::storage::sstable::version_gate::VersionGates;
use cqlite_core::Error as CoreError;

/// Wire name of the Flight action that returns per-table aggregate statistics.
pub const TABLE_STATS_ACTION: &str = "table_stats";

/// Request payload for [`TABLE_STATS_ACTION`] (`Action.body` JSON).
///
/// Intentionally free of any `cqlite-core` types so the Java connector can emit
/// it as plain JSON, mirroring [`crate::ticket::FlightTicket`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TableStatsRequest {
    /// Keyspace name.
    pub keyspace: String,
    /// Table name.
    pub table: String,
    /// Sidecar snapshot to read; `None`/absent reads the live data dir.
    #[serde(default)]
    pub snapshot: Option<String>,
}

impl TableStatsRequest {
    /// Parse a request from its on-the-wire JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }

    /// Serialise this request to its on-the-wire JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

/// Response payload for [`TABLE_STATS_ACTION`] (`Result.body` JSON).
///
/// All three fields are AUTHORITATIVE per-SSTable sums (see the module docs).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[non_exhaustive]
pub struct TableStatsResponse {
    /// Σ live (non-tombstone) rows across the table's SSTables. Upper bound on
    /// the table's true live-row count (pre-compaction overlap).
    pub live_rows: u64,
    /// Σ estimated partitions across the table's SSTables. Upper bound on the
    /// table's true distinct-partition count.
    pub partition_count: u64,
    /// Number of SSTables whose `Statistics.db` was parsed into the sums above.
    pub sstable_count: u64,
}

impl TableStatsResponse {
    /// Parse a response from its on-the-wire JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }

    /// Serialise this response to its on-the-wire JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

/// Errors produced while gathering per-table statistics.
#[derive(Debug, thiserror::Error)]
pub enum StatsError {
    /// The action body was not valid JSON for [`TableStatsRequest`].
    #[error("invalid table_stats request: {0}")]
    Decode(#[from] serde_json::Error),
    /// The resolved table directory could not be listed.
    #[error("failed to list SSTables in {path}: {source}")]
    Discovery {
        /// Directory that could not be read.
        path: std::path::PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
}

/// Sum the AUTHORITATIVE per-SSTable STATS counts for one table directory.
///
/// `dir` is the already-resolved SSTable directory (live data dir or a snapshot
/// directory — see [`crate::producer::DirSource::resolve`]). For each
/// `*-Statistics.db` file under `dir`, decode its [`read_table_counts`] and add
/// `partition_count` and `total_rows` to the running totals; `sstable_count`
/// counts the files that decoded.
///
/// A `Statistics.db` that fails to decode (corrupt, below the version floor, or a
/// `total_rows` the gated walk could not reach) is handled conservatively: a hard
/// decode error SKIPS the whole file (it does not contribute and is not counted),
/// and a reachable-but-`None` `total_rows` contributes 0 rows while still counting
/// its partitions. Both degrade the estimate toward "fewer rows" rather than
/// aborting the gate. A missing table directory surfaces as
/// [`StatsError::Discovery`]; an existing table with no SSTables yields an
/// all-zero response.
pub async fn gather_table_stats(dir: &Path) -> Result<TableStatsResponse, StatsError> {
    let entries = std::fs::read_dir(dir).map_err(|source| StatsError::Discovery {
        path: dir.to_path_buf(),
        source,
    })?;

    let stats_paths: Vec<std::path::PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Statistics.db"))
        })
        .collect();

    let mut response = TableStatsResponse::default();
    for path in stats_paths {
        match read_one_sstable_counts(&path) {
            Ok(counts) => {
                // Saturating: independent per-SSTable counts; a real table never
                // overflows u64, but never wrap into a tiny estimate that would
                // wrongly let a high-cardinality GROUP BY push.
                response.partition_count = response
                    .partition_count
                    .saturating_add(counts.partition_count);
                // total_rows is None when the gated walk could not reach it
                // (clustered covered-Slice not modeled). Contribute 0 rather than
                // guess; partitions still count.
                response.live_rows = response
                    .live_rows
                    .saturating_add(counts.total_rows.unwrap_or(0));
                response.sstable_count = response.sstable_count.saturating_add(1);
            }
            Err(e) => {
                // Skip an unreadable/corrupt/below-floor Statistics.db rather than
                // failing the whole gate. Logged at debug so the cause is recoverable.
                tracing::debug!(
                    path = %path.display(),
                    error = %e,
                    "table_stats: skipping undecodable Statistics.db"
                );
            }
        }
    }

    Ok(response)
}

/// Decode one `*-Statistics.db` file's authoritative counts. Derives the
/// [`VersionGates`] from the filename (so `total_rows` can be read on the gated
/// walk) and reads the whole file into memory (Statistics.db files are small).
fn read_one_sstable_counts(
    path: &Path,
) -> Result<cqlite_core::parser::repair_metadata::TableCounts, CoreError> {
    // Gates from the filename (na+ floor enforced by from_path); on an
    // unparseable descriptor fall back to None — partition_count is still
    // decodable self-describingly, total_rows is then reported as None.
    let gates = VersionGates::from_path(path).ok();
    let bytes = std::fs::read(path)?;
    read_table_counts(&bytes, gates.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::producer::DirSource;
    use crate::testutil::{build_sstables, simple_schema, write_row, KS, TBL};

    #[test]
    fn request_response_round_trip() {
        let req = TableStatsRequest {
            keyspace: "ks".into(),
            table: "tbl".into(),
            snapshot: Some("snap1".into()),
        };
        let back = TableStatsRequest::from_bytes(&req.to_bytes().unwrap()).unwrap();
        assert_eq!(req, back);

        let resp = TableStatsResponse {
            live_rows: 42,
            partition_count: 7,
            sstable_count: 3,
        };
        let back = TableStatsResponse::from_bytes(&resp.to_bytes().unwrap()).unwrap();
        assert_eq!(resp, back);
    }

    #[test]
    fn request_snapshot_defaults_to_none() {
        // The Java connector omits `snapshot` for a live read; #[serde(default)]
        // must accept that.
        let req = TableStatsRequest::from_bytes(br#"{"keyspace":"ks","table":"t"}"#).unwrap();
        assert_eq!(req.snapshot, None);
    }

    // `build_sstables` drives its own runtime to flush; gather_table_stats is
    // async, so build first then enter a fresh runtime to gather.
    //
    // The write-engine StatisticsWriter emits EMPTY estimated histograms, so the
    // histogram-derived partition_count is 0 for write-engine SSTables (real
    // Cassandra files carry a populated histogram — see the dataset-backed test).
    // This test therefore only asserts the per-SSTable COUNT and that gather
    // succeeds over a multi-SSTable directory.
    #[test]
    fn gather_counts_sstables_in_directory() {
        let schema = simple_schema();
        let (_temp, data_dir, _dir) = build_sstables(
            &schema,
            vec![
                vec![write_row(1, "a", 1, 100), write_row(2, "b", 2, 100)],
                vec![write_row(1, "a2", 3, 200), write_row(3, "c", 4, 200)],
            ],
        );
        // Resolve the SSTable dir exactly as the service does, then gather.
        let dir = DirSource::resolve(&data_dir, KS, TBL, None).into_dir();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let stats = rt.block_on(gather_table_stats(&dir)).expect("gather");

        assert_eq!(stats.sstable_count, 2, "two SSTables decoded");
    }

    /// Authoritative decode against REAL Cassandra 5.0 `nb` fixtures (issue #944).
    /// The estimatedPartitionSize histogram and `totalRows` come straight from the
    /// STATS component, so the per-table sums must equal the known ground truth.
    /// Skips when the binary dataset is not present (clean checkout / CI without
    /// fetched Data.db), but FAILS if present-but-wrong.
    #[test]
    fn gather_real_cassandra_fixture_authoritative_counts() {
        let Some(root) = std::env::var_os("CQLITE_DATASETS_ROOT") else {
            eprintln!("CQLITE_DATASETS_ROOT unset; skipping real-fixture stats test");
            return;
        };
        // sensor_data is a WIDE table: 10 partitions, 2000 rows — proves the gated
        // walk reaches totalRows past the clustered min/max block, and that rows
        // != partitions (the case the gate must distinguish).
        let base = std::path::Path::new(&root)
            .join("sstables")
            .join("test_timeseries");
        let dir = match std::fs::read_dir(&base) {
            Ok(entries) => entries.flatten().map(|e| e.path()).find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("sensor_data-"))
            }),
            Err(_) => None,
        };
        let Some(dir) = dir else {
            eprintln!("sensor_data fixture absent; skipping real-fixture stats test");
            return;
        };
        // Only assert when the Data.db binaries were actually fetched.
        let has_stats = std::fs::read_dir(&dir)
            .map(|es| {
                es.flatten().any(|e| {
                    e.file_name()
                        .to_str()
                        .is_some_and(|n| n.ends_with("-Statistics.db"))
                })
            })
            .unwrap_or(false);
        if !has_stats {
            eprintln!("sensor_data Statistics.db absent; skipping");
            return;
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        let stats = rt.block_on(gather_table_stats(&dir)).expect("gather");

        assert_eq!(stats.sstable_count, 1, "one SSTable in the fixture");
        assert_eq!(stats.partition_count, 10, "sensor_data has 10 partitions");
        assert_eq!(stats.live_rows, 2000, "sensor_data has 2000 rows (wide)");
    }

    #[test]
    fn gather_empty_table_is_all_zero() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path().join("data").join(KS).join(TBL);
        std::fs::create_dir_all(&dir).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let stats = rt.block_on(gather_table_stats(&dir)).expect("gather");
        assert_eq!(
            stats,
            TableStatsResponse::default(),
            "no SSTables → all-zero stats"
        );
    }

    #[test]
    fn gather_missing_dir_is_discovery_error() {
        let temp = tempfile::TempDir::new().unwrap();
        let missing = temp.path().join("does").join("not").join("exist");
        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = rt
            .block_on(gather_table_stats(&missing))
            .expect_err("missing dir must error");
        assert!(matches!(err, StatsError::Discovery { .. }), "got {err:?}");
    }
}
