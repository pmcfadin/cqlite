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
//! - `sstable_count` = number of SSTables whose sibling `*-Statistics.db` was
//!   successfully decoded. The SSTable SET is enumerated from the authoritative
//!   `*-Data.db` components (the "this SSTable exists" signal); a Data.db whose
//!   sibling Statistics.db is missing or undecodable TAINTS completeness instead
//!   of being silently invisible (issue #944, #28).
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
/// All three counts are AUTHORITATIVE per-SSTable sums (see the module docs).
/// The `complete` flag tells the consumer whether those sums cover EVERY SSTable
/// in the directory: partial sums are NOT authoritative (no-heuristics mandate,
/// issue #28) and the consumer must fail closed to "no estimate" rather than feed
/// a biased ratio to the AUTOMATIC pushdown gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// `true` iff EVERY `*-Statistics.db` in the directory decoded into the sums
    /// above. `false` if ANY file failed to read/decode (corrupt, below the
    /// version floor, or an undecodable descriptor) — the counts are then an
    /// explicit UNDER-count over only the SSTables that decoded, and the consumer
    /// MUST treat the estimate as unknown rather than compute a (biased) ratio.
    ///
    /// Defaults to `false` on deserialization so a response from an older server
    /// that predates this field is treated as incomplete (fail closed), never as
    /// spuriously complete.
    #[serde(default = "default_incomplete")]
    pub complete: bool,
    /// Number of `*-Statistics.db` files that failed to read/decode and so did
    /// NOT contribute to the sums above (0 iff `complete`). Surfaced for
    /// diagnostics; the boolean `complete` is the authoritative gate signal.
    #[serde(default)]
    pub skipped_sstables: u64,
}

/// serde default for [`TableStatsResponse::complete`]: an absent flag (older
/// server) is treated as INCOMPLETE so the consumer fails closed to "no estimate".
fn default_incomplete() -> bool {
    false
}

impl Default for TableStatsResponse {
    fn default() -> Self {
        // An empty table (no SSTables at all) is trivially COMPLETE: there is
        // nothing that could have been skipped. `gather_table_stats` flips
        // `complete` to false only when it actually skips a file.
        Self {
            live_rows: 0,
            partition_count: 0,
            sstable_count: 0,
            complete: true,
            skipped_sstables: 0,
        }
    }
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
/// directory — see [`crate::producer::DirSource::resolve`]). The SSTable SET is
/// enumerated from the `*-Data.db` components (the authoritative "this SSTable
/// exists" signal). For each Data.db we derive its sibling `*-Statistics.db` path
/// (same generation prefix, e.g. `nb-<gen>-big-Data.db` → `nb-<gen>-big-Statistics.db`),
/// decode its [`read_table_counts`], and add `partition_count` and `total_rows` to
/// the running totals; `sstable_count` counts the SSTables whose stats decoded.
///
/// Enumerating by Data.db (not by Statistics.db) is what closes the fail-closed
/// hole in issue #944: an SSTable with a readable Data.db but a MISSING or
/// unreadable `*-Statistics.db` is no longer silently invisible — it now TAINTS
/// completeness (`complete=false`, `skipped_sstables` incremented), exactly like
/// the undecodable-stats path below.
///
/// A `Statistics.db` that is missing, fails to read, or fails to decode (corrupt,
/// below the version floor) is handled conservatively: the SSTable does not
/// contribute and is not counted, AND the response's
/// [`TableStatsResponse::complete`] flag flips to `false` (with `skipped_sstables`
/// incremented). Partial sums are NOT authoritative (issue #28), so the consumer
/// must fail closed to "no estimate" rather than feed a biased ratio to the
/// AUTOMATIC pushdown gate — the flag is how it learns the totals are incomplete.
///
/// A file that decodes but whose `total_rows` is `None` (the gated walk could not
/// reach it — e.g. a clustered covered-Slice not modeled) ALSO marks the response
/// incomplete (`complete=false`, `skipped_sstables` incremented). The
/// authoritative-or-nothing principle (issue #28) means a missing row count cannot
/// be reported as complete: `live_rows` is what the gate/optimizer divide by, so a
/// silent 0-row contribution would under-count `live_rows` while still claiming to
/// be authoritative. Its `partition_count` is still summed (the
/// `estimatedPartitionSize` histogram is self-describing and authoritative), but the
/// completeness flag must be `false` so the consumer fails closed. A missing table
/// directory surfaces as [`StatsError::Discovery`]; an existing table with no
/// SSTables yields an all-zero `complete=true` response (nothing was skipped).
///
/// This is a SYNCHRONOUS, blocking function: it performs `std::fs::read_dir` /
/// `std::fs::read` directly. Callers on an async runtime MUST invoke it inside
/// [`tokio::task::spawn_blocking`] so the blocking I/O cannot stall the reactor
/// (see [`crate::service`] `do_action_inner`, matching the `do_get` merge offload).
pub fn gather_table_stats(dir: &Path) -> Result<TableStatsResponse, StatsError> {
    let entries = std::fs::read_dir(dir).map_err(|source| StatsError::Discovery {
        path: dir.to_path_buf(),
        source,
    })?;

    let mut response = TableStatsResponse::default();

    // Enumerate the SSTable SET from the `*-Data.db` components (the authoritative
    // "this SSTable exists" signal), WITHOUT silently dropping iteration errors. A
    // `read_dir` entry that fails to yield (e.g. a transient I/O error partway
    // through) means we may NOT have seen every SSTable, so the totals cannot be
    // authoritative: taint completeness (consistent with the missing/undecodable
    // sibling-Statistics.db and missing-`total_rows` paths below) rather than
    // claiming complete totals over files we could not enumerate (issue #944, #28).
    let data_paths = collect_data_paths(entries, &mut response);

    for data_path in data_paths {
        // Derive the sibling Statistics.db (same generation prefix). The lookup
        // itself can fail (no recognisable Data.db suffix) — that taints too.
        let stats_path = match sibling_statistics_path(&data_path) {
            Some(p) => p,
            None => {
                response.complete = false;
                response.skipped_sstables = response.skipped_sstables.saturating_add(1);
                tracing::debug!(
                    path = %data_path.display(),
                    "table_stats: Data.db with no derivable Statistics.db sibling (marking incomplete)"
                );
                continue;
            }
        };
        match read_one_sstable_counts(&stats_path) {
            Ok(counts) => fold_counts(&mut response, &stats_path, counts),
            Err(e) => {
                // A sibling Statistics.db that is MISSING (NotFound), unreadable, or
                // undecodable (corrupt, below the version floor) does not contribute
                // and is not counted, but marks the totals INCOMPLETE: partial sums
                // are not authoritative (issue #28), so the consumer must fail closed
                // to "no estimate" instead of feeding a biased ratio to the AUTOMATIC
                // gate. Enumerating by Data.db means a Data.db with no usable stats
                // now TAINTS instead of being silently invisible. Logged at debug so
                // the cause is recoverable.
                response.complete = false;
                response.skipped_sstables = response.skipped_sstables.saturating_add(1);
                tracing::debug!(
                    data_path = %data_path.display(),
                    stats_path = %stats_path.display(),
                    error = %e,
                    "table_stats: skipping missing/undecodable sibling Statistics.db (marking incomplete)"
                );
            }
        }
    }

    Ok(response)
}

/// Derive the sibling `*-Statistics.db` path for an SSTable's `*-Data.db` path by
/// swapping the trailing `Data.db` component for `Statistics.db` (the generation
/// prefix — e.g. `nb-<gen>-big-` — is identical for every component of one
/// SSTable). Returns `None` if `data_path` does not end in `-Data.db`.
fn sibling_statistics_path(data_path: &Path) -> Option<std::path::PathBuf> {
    let name = data_path.file_name().and_then(|n| n.to_str())?;
    let prefix = name.strip_suffix("-Data.db")?;
    let stats_name = format!("{prefix}-Statistics.db");
    Some(data_path.with_file_name(stats_name))
}

/// Collect the `*-Data.db` paths from a `read_dir` iterator, treating any
/// entry-iteration error as INCOMPLETE rather than silently discarding it.
///
/// The SSTable SET is enumerated from `*-Data.db` (the authoritative "this SSTable
/// exists" signal) rather than from `*-Statistics.db`: an SSTable with a readable
/// Data.db but a missing/unreadable Statistics.db must TAINT completeness, not be
/// silently invisible (issue #944, #28). `read_dir` yields `io::Result<DirEntry>`;
/// an `Err` element means an entry could not be read (e.g. a transient FS error
/// partway through enumeration), so the directory listing is NOT known to be
/// exhaustive. Each such error flips `response.complete` to `false` and bumps
/// `skipped_sstables` — the consumer then fails closed to "no estimate" instead of
/// reporting authoritative totals over a directory it could not fully enumerate.
fn collect_data_paths<I>(entries: I, response: &mut TableStatsResponse) -> Vec<std::path::PathBuf>
where
    I: IntoIterator<Item = std::io::Result<std::fs::DirEntry>>,
{
    let mut paths = Vec::new();
    for entry in entries {
        match entry {
            Ok(e) => {
                let path = e.path();
                if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with("-Data.db"))
                {
                    paths.push(path);
                }
            }
            Err(e) => {
                // An unreadable directory entry: the listing is not known to be
                // complete, so taint the totals rather than dropping the error.
                response.complete = false;
                response.skipped_sstables = response.skipped_sstables.saturating_add(1);
                tracing::debug!(
                    error = %e,
                    "table_stats: read_dir entry iteration error (marking incomplete)"
                );
            }
        }
    }
    paths
}

/// Fold one decoded SSTable's [`TableCounts`] into the running `response`.
///
/// `partition_count` (from the self-describing `estimatedPartitionSize` histogram)
/// is always authoritative and is summed unconditionally. `total_rows`, however, is
/// authoritative-or-nothing: when it is `None` (the gated walk could not reach the
/// `totalRows` field — e.g. a clustered covered-Slice not modeled) the response is
/// marked INCOMPLETE and the file counted toward `skipped_sstables`, rather than
/// silently contributing 0 live rows while still claiming `complete`. `live_rows` is
/// the denominator the gate/optimizer divide by, so an honest 0 here would under-
/// count and mislead the consumer — issue #28's authoritative-or-nothing rule (this
/// REVERSES the earlier choice to stay complete on `None`). Only a file with a known
/// row count contributes to `live_rows` and `sstable_count`.
fn fold_counts(
    response: &mut TableStatsResponse,
    path: &Path,
    counts: cqlite_core::parser::repair_metadata::TableCounts,
) {
    // Saturating: independent per-SSTable counts; a real table never overflows u64,
    // but never wrap into a tiny estimate that would wrongly let a high-cardinality
    // GROUP BY push.
    response.partition_count = response
        .partition_count
        .saturating_add(counts.partition_count);
    match counts.total_rows {
        Some(rows) => {
            response.live_rows = response.live_rows.saturating_add(rows);
            response.sstable_count = response.sstable_count.saturating_add(1);
        }
        None => {
            response.complete = false;
            response.skipped_sstables = response.skipped_sstables.saturating_add(1);
            tracing::debug!(
                path = %path.display(),
                "table_stats: Statistics.db decoded but total_rows is None (marking incomplete)"
            );
        }
    }
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
            complete: false,
            skipped_sstables: 1,
            ..Default::default()
        };
        let back = TableStatsResponse::from_bytes(&resp.to_bytes().unwrap()).unwrap();
        assert_eq!(resp, back);
    }

    #[test]
    fn response_complete_defaults_false_when_flag_absent() {
        // A response from an older server that predates the `complete` field must
        // deserialize as INCOMPLETE (fail closed), never spuriously complete.
        let resp = TableStatsResponse::from_bytes(
            br#"{"live_rows":5,"partition_count":2,"sstable_count":1}"#,
        )
        .unwrap();
        assert_eq!(resp.live_rows, 5);
        assert!(!resp.complete, "absent complete flag must default to false");
        assert_eq!(resp.skipped_sstables, 0);
    }

    #[test]
    fn request_snapshot_defaults_to_none() {
        // The Java connector omits `snapshot` for a live read; #[serde(default)]
        // must accept that.
        let req = TableStatsRequest::from_bytes(br#"{"keyspace":"ks","table":"t"}"#).unwrap();
        assert_eq!(req.snapshot, None);
    }

    // `build_sstables` drives its own runtime to flush; gather_table_stats is
    // synchronous (callers offload it via spawn_blocking), so build first then
    // gather directly.
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

        let stats = gather_table_stats(&dir).expect("gather");

        assert_eq!(stats.sstable_count, 2, "two SSTables decoded");
        assert!(stats.complete, "all SSTables decoded → complete");
        assert_eq!(stats.skipped_sstables, 0);
    }

    /// A directory containing an UNDECODABLE `Statistics.db` must mark the response
    /// INCOMPLETE (`complete=false`, `skipped_sstables` incremented) so the consumer
    /// fails closed to "no estimate" instead of feeding a biased ratio to the gate
    /// (no-heuristics mandate, issue #28). A sibling fully-decodable directory must
    /// yield `complete=true`.
    #[test]
    fn gather_undecodable_statistics_marks_incomplete() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path().join("data").join(KS).join(TBL);
        std::fs::create_dir_all(&dir).unwrap();
        // A Data.db enumerates the SSTable; its sibling Statistics.db holds garbage
        // bytes that read_table_counts cannot decode, so read_one_sstable_counts
        // returns Err and the SSTable is skipped + marks the response incomplete.
        std::fs::write(dir.join("nb-1-big-Data.db"), b"data").unwrap();
        std::fs::write(
            dir.join("nb-1-big-Statistics.db"),
            b"not a real statistics file",
        )
        .unwrap();

        let stats = gather_table_stats(&dir).expect("gather");

        assert!(
            !stats.complete,
            "an undecodable Statistics.db must mark the response incomplete"
        );
        assert_eq!(
            stats.skipped_sstables, 1,
            "the corrupt file is counted as skipped"
        );
        assert_eq!(
            stats.sstable_count, 0,
            "the corrupt file did not contribute"
        );
    }

    /// A directory containing a `*-Data.db` with NO sibling `*-Statistics.db` must
    /// mark the response INCOMPLETE (`complete=false`, `skipped_sstables`
    /// incremented) — the SSTable is enumerated by its Data.db, so a missing
    /// Statistics.db can no longer make it silently invisible (issue #944, #28).
    #[test]
    fn gather_data_db_without_statistics_sibling_taints() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path().join("data").join(KS).join(TBL);
        std::fs::create_dir_all(&dir).unwrap();
        // A readable Data.db with NO sibling Statistics.db. The old enumeration (by
        // Statistics.db) would have seen an empty directory and reported
        // complete=true with zero counts — silently hiding this SSTable.
        std::fs::write(dir.join("nb-1-big-Data.db"), b"data").unwrap();

        let stats = gather_table_stats(&dir).expect("gather");

        assert!(
            !stats.complete,
            "a Data.db with no Statistics.db sibling must mark the response incomplete"
        );
        assert_eq!(
            stats.skipped_sstables, 1,
            "the SSTable with no usable stats is counted as skipped"
        );
        assert_eq!(
            stats.sstable_count, 0,
            "the SSTable with no usable stats did not contribute"
        );
    }

    /// A fully-decodable directory yields `complete=true`. Uses the write-engine
    /// build path (its StatisticsWriter still emits a decodable STATS component even
    /// though the histogram is empty), confirming a clean decode is reported complete.
    #[test]
    fn gather_fully_decodable_directory_is_complete() {
        let schema = simple_schema();
        let (_temp, data_dir, _dir) = build_sstables(
            &schema,
            vec![vec![write_row(1, "a", 1, 100), write_row(2, "b", 2, 100)]],
        );
        let dir = DirSource::resolve(&data_dir, KS, TBL, None).into_dir();

        let stats = gather_table_stats(&dir).expect("gather");

        assert!(stats.complete, "every Statistics.db decoded → complete");
        assert_eq!(stats.skipped_sstables, 0);
    }

    /// A directory where one SSTable's `total_rows` is `None` (but a sibling has a
    /// known row count) must report `complete=false` (issue #944, #28: a missing row
    /// count is authoritative-or-nothing — `live_rows` is what the gate/optimizer
    /// divide by, so a silent 0 contribution must not be reported as complete). The
    /// known-rows SSTable still contributes its `live_rows`/`sstable_count`, and both
    /// SSTables contribute their authoritative `partition_count`; the None one is
    /// counted toward `skipped_sstables`. This exercises [`fold_counts`] directly
    /// (the per-file decode→fold seam) so the rule is tested without crafting a STATS
    /// file whose descriptor parses but whose gated walk cannot reach `totalRows`.
    #[test]
    fn fold_none_total_rows_marks_incomplete() {
        use cqlite_core::parser::repair_metadata::TableCounts;
        let dummy = std::path::Path::new("nb-2-big-Statistics.db");

        let mut response = TableStatsResponse::default();
        // SSTable A: fully decoded, 5 rows over 2 partitions.
        fold_counts(
            &mut response,
            std::path::Path::new("nb-1-big-Statistics.db"),
            TableCounts {
                partition_count: 2,
                total_rows: Some(5),
            },
        );
        // SSTable B: histogram decoded (3 partitions) but the gated walk could not
        // reach totalRows → None.
        fold_counts(
            &mut response,
            dummy,
            TableCounts {
                partition_count: 3,
                total_rows: None,
            },
        );

        assert!(
            !response.complete,
            "any SSTable with total_rows=None must mark the response incomplete"
        );
        assert_eq!(
            response.live_rows, 5,
            "only the known-rows SSTable contributes live_rows"
        );
        assert_eq!(
            response.partition_count, 5,
            "partition_count is authoritative for BOTH SSTables (2 + 3)"
        );
        assert_eq!(
            response.sstable_count, 1,
            "only the SSTable with a known row count is counted as contributing"
        );
        assert_eq!(
            response.skipped_sstables, 1,
            "the None-total_rows SSTable is counted as skipped"
        );
    }

    /// The all-rows-present case stays `complete=true`: two SSTables, each with a
    /// known `total_rows`, fold to a complete response with summed counts.
    #[test]
    fn fold_all_rows_present_stays_complete() {
        use cqlite_core::parser::repair_metadata::TableCounts;
        let mut response = TableStatsResponse::default();
        fold_counts(
            &mut response,
            std::path::Path::new("nb-1-big-Statistics.db"),
            TableCounts {
                partition_count: 2,
                total_rows: Some(5),
            },
        );
        fold_counts(
            &mut response,
            std::path::Path::new("nb-2-big-Statistics.db"),
            TableCounts {
                partition_count: 3,
                total_rows: Some(7),
            },
        );

        assert!(
            response.complete,
            "all SSTables have a known row count → complete"
        );
        assert_eq!(response.live_rows, 12);
        assert_eq!(response.partition_count, 5);
        assert_eq!(response.sstable_count, 2);
        assert_eq!(response.skipped_sstables, 0);
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

        let stats = gather_table_stats(&dir).expect("gather");

        assert_eq!(stats.sstable_count, 1, "one SSTable in the fixture");
        assert_eq!(stats.partition_count, 10, "sensor_data has 10 partitions");
        assert_eq!(stats.live_rows, 2000, "sensor_data has 2000 rows (wide)");
        assert!(stats.complete, "real fixture decoded fully → complete");
        assert_eq!(stats.skipped_sstables, 0);
    }

    #[test]
    fn gather_empty_table_is_all_zero() {
        let temp = tempfile::TempDir::new().unwrap();
        let dir = temp.path().join("data").join(KS).join(TBL);
        std::fs::create_dir_all(&dir).unwrap();
        let stats = gather_table_stats(&dir).expect("gather");
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
        let err = gather_table_stats(&missing).expect_err("missing dir must error");
        assert!(matches!(err, StatsError::Discovery { .. }), "got {err:?}");
    }

    #[test]
    fn read_dir_entry_error_taints_completeness() {
        // A `read_dir` iterator that yields an Err entry partway through must NOT be
        // silently dropped: the directory listing is not known to be exhaustive, so
        // the totals are tainted INCOMPLETE (issue #944, #28). We drive the helper
        // directly with a synthetic iterator since provoking a real entry-iteration
        // error from the OS is not portable.
        let entries: Vec<std::io::Result<std::fs::DirEntry>> = vec![Err(std::io::Error::other(
            "synthetic entry iteration failure",
        ))];
        let mut response = TableStatsResponse::default();
        let paths = collect_data_paths(entries, &mut response);

        assert!(paths.is_empty(), "an errored entry yields no path");
        assert!(
            !response.complete,
            "a read_dir entry error must taint completeness (no silent drop)"
        );
        assert_eq!(
            response.skipped_sstables, 1,
            "the unreadable entry is visible as a skip"
        );
    }

    #[test]
    fn sibling_statistics_path_swaps_suffix() {
        // The generation prefix is shared across an SSTable's components: a Data.db
        // maps to its sibling Statistics.db by swapping only the trailing suffix.
        let data = std::path::Path::new("/x/y/nb-7-big-Data.db");
        assert_eq!(
            sibling_statistics_path(data).unwrap(),
            std::path::PathBuf::from("/x/y/nb-7-big-Statistics.db")
        );
        // A path that is not a Data.db has no derivable sibling.
        assert!(sibling_statistics_path(std::path::Path::new("/x/y/nb-7-big-Index.db")).is_none());
    }

    #[test]
    fn read_dir_all_ok_entries_stay_complete() {
        // Sanity: with no Err entries the helper leaves completeness untouched. We
        // enumerate a real (empty) directory so every yielded item is Ok.
        let temp = tempfile::TempDir::new().unwrap();
        let entries = std::fs::read_dir(temp.path()).unwrap();
        let mut response = TableStatsResponse::default();
        let paths = collect_data_paths(entries, &mut response);

        assert!(paths.is_empty());
        assert!(response.complete, "all-Ok enumeration stays complete");
        assert_eq!(response.skipped_sstables, 0);
    }
}
