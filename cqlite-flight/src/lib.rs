//! Arrow Flight server for CQLite.
//!
//! Exposes Cassandra SSTable data to Arrow Flight clients (e.g. a Trino
//! connector). On read, the server performs an on-the-fly compaction merge of a
//! node's SSTables — leaving the originals untouched — applies token-range and
//! predicate filters, and streams the result as Arrow record batches.
//!
//! See `docs/flight-trino/PLAN.md` for the full design and `JOURNAL.md` for the
//! change log.

pub mod agg;
pub mod cancel;
pub mod filter;
pub mod obs;
pub mod pathsafe;
pub mod producer;
pub mod service;
pub mod scan_progress;
pub mod shutdown;
pub mod stats;
pub mod streaming;
pub mod ticket;

#[cfg(test)]
mod testutil;

// Issue #2162: OTel-level assertions (phase histograms, bounded attributes,
// rpc.rows presence) through the shared `observability-testing` capture harness.
// Gated behind the feature (which pulls in the SDK in-memory exporters) so the
// default build never links OTel; the feature-independent `StreamProbe` seam
// tests in `streaming_tests.rs` carry the always-compiled wiring evidence.
#[cfg(all(test, feature = "observability-testing"))]
#[path = "metrics_capture_tests.rs"]
mod metrics_capture_tests;
