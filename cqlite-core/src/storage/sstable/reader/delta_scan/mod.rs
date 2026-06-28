//! Delta-scan record model and streaming API for CDC-style Parquet projections
//! (Epic #696, Issue #697 types, Issue #698 `scan_delta` implementation).
//!
//! A flushed SSTable is a delta, not a snapshot.  Projecting it to Parquet
//! correctly requires carrying per-cell write-timestamps and representing every
//! delete shape Cassandra produces — otherwise a downstream union of per-flush
//! files resurrects deleted data and merges stale cells.
//!
//! ## Design contract
//!
//! One SSTable generation in, faithful change events out.  Reconciliation
//! (LWW merge, tombstone application, TTL filtering) is deliberately the
//! downstream consumer's responsibility.
//!
//! ## Types
//!
//! | Type | Purpose |
//! |------|---------|
//! | [`DeltaRecord`] | Discriminated union of every change shape |
//! | [`CellDelta`] | Per-cell value + writetime + TTL + collection flag |
//! | [`RangeBound`] | Typed, possibly-prefix clustering-key bound |
//! | [`RowKeys`] | Partition key + optional clustering columns |
//! | [`CellMeta`] | Row liveness metadata (timestamp, TTL) |
//!
//! ## Streaming API
//!
//! [`scan_delta`] streams [`DeltaRecord`]s from a single SSTable generation
//! (one `Data.db` file) via a [`tokio::sync::mpsc`] channel.  It lives on the
//! reader layer and does **not** route through the query engine (which merges
//! generations and suppresses tombstones — the opposite contract).
//!
//! Row/range/partition tombstone emission is Issue #699 scope.  This version
//! errors or returns early on delete-bearing input; upsert and static-upsert
//! paths are complete and correct.
//!
//! ## Module layout
//!
//! - [`model`] — the data-model types (record/cell/key/bound structs and the
//!   scan-summary handle).
//! - [`scan`] — the [`scan_delta`] streaming driver and its parse/emit closure.
//!
//! ## Feature gate
//!
//! Everything in this module is behind `feature = "delta-scan"` and will not
//! compile into the default crate build.

mod model;
mod scan;

pub use model::{
    CellDelta, CellMeta, DeltaRecord, RangeBound, RowKeys, ScanSummary, ScanSummaryHandle,
};
pub use scan::{scan_delta, ScanDeltaOutput};
