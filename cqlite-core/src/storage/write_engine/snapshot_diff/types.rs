//! Report and input types for the two-snapshot diff (issue #4201; design.md §D4).
//!
//! Everything here is plain data. No type in this file reads a file or decides a verdict.

use serde::Serialize;
use std::path::PathBuf;

/// Which of the two compared snapshots a fact belongs to. Serialises as `"a"` / `"b"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    A,
    B,
}

impl Side {
    /// The other snapshot.
    #[must_use]
    pub fn other(self) -> Self {
        match self {
            Side::A => Side::B,
            Side::B => Side::A,
        }
    }

    /// `"a"` or `"b"` (the label used in messages and the report).
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Side::A => "a",
            Side::B => "b",
        }
    }
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// One compared snapshot: its directory plus the live generations the caller resolved in it,
/// **newest first** (`run_index` 0 is the newest generation, the last-write-wins tie-break rank
/// every other merger caller uses). The CLI resolves both with
/// `explain_resolve::{resolve_table_dir, resolve_generations}`.
#[derive(Debug, Clone)]
pub struct DiffSide {
    /// The flat table directory (kept for the report only; never read or written here).
    pub dir: PathBuf,
    /// `*-Data.db` paths of the side's live generations, newest first.
    pub generations: Vec<PathBuf>,
    /// The side's generation names for the report header, OLDEST first, as the caller wants them
    /// shown (the CLI passes the generation module's names, `(toc-missing)` suffix included).
    /// Empty: derive them from `generations`' file names.
    pub listed_names: Vec<String>,
}

/// Knobs for one diff run.
#[derive(Debug, Clone)]
pub struct DiffOptions {
    /// The pinned reconciliation clock (epoch seconds), identical for both sides.
    pub now_secs: i64,
    /// The materialization budget for the report (the same `max_result_bytes` the query surface
    /// honours). Exceeding it is [`crate::Error::ResultTooLarge`], never a truncated report.
    pub max_result_bytes: u64,
}

/// A side's directory and generation basenames, for the report header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SideInfo {
    pub dir: String,
    pub generations: Vec<String>,
}

/// The status of one `(clustering, column)` cell across the two sides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CellStatus {
    Equal,
    OnlyA,
    OnlyB,
    NewerInA,
    NewerInB,
    /// Same key, same write timestamp, differing content (owner Q21=b); see `resolves_to`.
    Conflict,
}

/// One side's reconciled winner for a cell.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CellSide {
    pub value: String,
    /// RFC3339 with up to microsecond precision.
    pub writetime: String,
    /// The cell's TTL in seconds, for an expiring cell.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<u32>,
    /// RFC3339 expiry of an expiring cell.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    /// RFC3339 local deletion time of a tombstone side.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_deletion_time: Option<String>,
}

/// One compared cell.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CellDiffEntry {
    pub clustering: Option<String>,
    pub column: String,
    /// Hex of a collection element's path bytes; absent for a simple cell or a frozen value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cell_path: Option<String>,
    pub status: CellStatus,
    /// On `conflict` only: the side `Cells.resolveRegular` keeps (cassandra-5.0.8
    /// `db/rows/Cells.java:79-128`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolves_to: Option<Side>,
    pub a: Option<CellSide>,
    pub b: Option<CellSide>,
}

/// A cell on the OTHER side that a tombstone would shadow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ShadowedCell {
    pub clustering: Option<String>,
    pub column: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cell_path: Option<String>,
    pub side: Side,
}

/// `resurrection_risk` on a tombstone. Always present (design.md §D4): `"no"` means checked and
/// nothing is shadowed, never "not computed".
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResurrectionRisk {
    /// `"yes"` or `"no"`.
    pub status: &'static str,
    /// `"before-gc-grace"` / `"past-gc-grace"`; present only when `status == "yes"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<&'static str>,
    /// Every shadowed cell on the other side, named (never just counted).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub shadowed_cells: Vec<ShadowedCell>,
}

/// One tombstone present on exactly one side (a tombstone both sides hold is omitted).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TombstoneDiffEntry {
    /// `partition` / `range` / `row` / `cell` / `collection`.
    pub kind: &'static str,
    pub clustering: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// Hex of the element path of a cell tombstone on a collection element.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cell_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range_start: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range_end: Option<String>,
    /// The side that holds it.
    pub side: Side,
    pub deletion_time: String,
    pub local_deletion_time: String,
    pub resurrection_risk: ResurrectionRisk,
}

/// Everything found in one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PartitionDiff {
    pub partition_key: String,
    pub cells: Vec<CellDiffEntry>,
    pub tombstones: Vec<TombstoneDiffEntry>,
}

/// Counts over the whole report (design.md §D4).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct DiffSummary {
    pub partitions_compared: u64,
    pub equal: u64,
    pub conflicts: u64,
    pub only_a: u64,
    pub only_b: u64,
    pub newer_in_a: u64,
    pub newer_in_b: u64,
    pub tombstones_only_a: u64,
    pub tombstones_only_b: u64,
    pub resurrection_risk_before_gc_grace: u64,
    pub resurrection_risk_past_gc_grace: u64,
}

/// The full report. Field order is the JSON key order of design.md §D4.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiffReport {
    /// RFC3339 of the pinned clock.
    pub now: String,
    pub table: String,
    pub gc_grace_seconds: i64,
    pub side_a: SideInfo,
    pub side_b: SideInfo,
    pub partitions: Vec<PartitionDiff>,
    pub summary: DiffSummary,
}

/// Why a diff could not be computed. A failure on either side is never papered over with a report
/// built from a shorter trail (the #4159 class).
#[derive(Debug, thiserror::Error)]
pub enum DiffError {
    /// A generation on one side could not be read.
    #[error("side={side}: generation read failed for {path}: {source}")]
    SideRead {
        side: Side,
        /// The generation that failed, or every generation of the side when none could be singled
        /// out by re-opening them.
        path: String,
        #[source]
        source: crate::Error,
    },
    /// `now` is outside the GC-clock range (or `now - gc_grace_seconds` would overflow).
    #[error("--now {value} is out of range: {reason}")]
    InvalidNow { value: i64, reason: String },
    /// A cell value could not be serialized for comparison. The diff refuses rather than compare a
    /// partial or empty buffer (which could call two different values `equal`).
    #[error(
        "partition {partition} clustering {clustering} column {column}: the value cannot be \
         serialized for comparison: {source}"
    )]
    CellSerialization {
        partition: String,
        clustering: String,
        column: String,
        #[source]
        source: Box<crate::Error>,
    },
    /// Any other failure (schema, budget, partition-key encoding).
    #[error(transparent)]
    Core(#[from] crate::Error),
}
